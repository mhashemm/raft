package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	follower uint32 = iota
	leader
	disabled
)

const (
	heartbeatTickerDuration = 100 * time.Minute
)

type Server struct {
	State
	mu                     sync.Mutex
	log                    Log
	commitIndex            uint64
	lastApplied            uint64
	lastLogIndex           uint64
	lastLogTerm            uint64
	indexMu                sync.Mutex
	nextIndex              map[string]uint64
	matchIndex             map[string]uint64
	role                   atomic.Uint32
	logger                 *slog.Logger
	electionTickerDuration time.Duration
	electionTicker         *time.Ticker
	heartbeatTicker        *time.Ticker
	lastHeartBeat          time.Time
}

func randomDuration() time.Duration {
	return time.Duration((rand.IntN(150) + 150)) * time.Minute
}

func NewServer() *Server {
	s := State{}
	s.Init()
	l := Log{}
	l.Open(s.id)

	lastEntry := l.LastEntry()
	electionTickerDuration := randomDuration()
	heartbeatTicker := time.NewTicker(heartbeatTickerDuration)
	heartbeatTicker.Stop()

	server := &Server{
		State:                  s,
		log:                    l,
		lastLogIndex:           lastEntry.Index,
		lastLogTerm:            lastEntry.Term,
		nextIndex:              map[string]uint64{},
		matchIndex:             map[string]uint64{},
		logger:                 slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true})),
		electionTickerDuration: electionTickerDuration,
		electionTicker:         time.NewTicker(electionTickerDuration),
		heartbeatTicker:        heartbeatTicker,
		lastHeartBeat:          time.Unix(0, 0),
	}
	go server.electionWorker()
	go server.hearbeatWorker()
	return server
}

func (s *Server) switchToLeader() {
	s.role.Store(leader)
	s.electionTicker.Stop()
	s.electionTickerDuration = 0
	s.heartbeatTicker.Reset(heartbeatTickerDuration)
	for pid := range s.nextIndex {
		s.nextIndex[pid] = s.lastLogIndex + 1
	}
	for pid := range s.matchIndex {
		s.matchIndex[pid] = 0
	}
}
func (s *Server) switchToFollower() {
	s.role.Store(follower)
	s.heartbeatTicker.Stop()
	s.electionTickerDuration = randomDuration()
	s.electionTicker.Reset(s.electionTickerDuration)
}
func (s *Server) switchToDisabled() {
	s.role.Store(disabled)
	s.heartbeatTicker.Stop()
	s.electionTicker.Stop()
}

func (s *Server) hearbeatWorker() {
	for t := range s.heartbeatTicker.C {
		_ = t
		s.logger.Info("heartbeat", "tick", t, "role", s.role.Load())
		if time.Since(s.lastHeartBeat).Abs() < heartbeatTickerDuration {
			continue
		}
		s.mu.Lock()
		peers := maps.Clone(s.Peers)
		s.mu.Unlock()
		wg := sync.WaitGroup{}
		wg.Add(len(peers))
		c, cancel := context.WithTimeout(context.TODO(), time.Duration(300)*time.Millisecond)
		for pid, peer := range peers {
			go func() {
				defer wg.Done()
				req := &AppendEntriesRequest{
					Term:         s.CurrentTerm,
					LeaderId:     s.id,
					PrevLogIndex: s.lastLogIndex,
					LeaderCommit: s.commitIndex,
					PrevLogTerm:  s.lastLogTerm,
				}
				_, err := peer.client.AppendEntries(c, req)
				if err != nil {
					s.logger.Error(err.Error(), "pid", pid)
					return
				}
			}()
		}
		wg.Wait()
		cancel()
		s.mu.Lock()
		s.lastHeartBeat = time.Now()
		s.mu.Unlock()
	}
}

func (s *Server) electionWorker() {
	for t := range s.electionTicker.C {
		s.logger.Info("election tick", "tick", t, "role", s.role.Load())
		_ = t
		if s.role.Load() == leader || time.Since(s.lastHeartBeat) < s.electionTickerDuration {
			continue
		}
		s.mu.Lock()
		s.CurrentTerm += 1
		s.VotedFor = s.id
		wg := sync.WaitGroup{}
		wg.Add(len(s.Peers))
		c, cancel := context.WithTimeout(context.TODO(), time.Duration(300)*time.Millisecond)
		majority := (len(s.Peers) / 2) + 1
		votes := atomic.Int32{}
		votes.Add(1) //ourselves
		resTerms := make(chan uint64, len(s.Peers))
		for pid, peer := range s.Peers {
			go func() {
				defer wg.Done()
				req := &RequestVoteRequest{
					Term:         s.CurrentTerm,
					CandidateId:  s.id,
					LastLogIndex: s.lastLogIndex,
					LastLogTerm:  s.lastLogTerm,
				}
				res, err := peer.client.RequestVote(c, req)
				if err != nil {
					s.logger.Error(err.Error(), "pid", pid)
					return
				}
				resTerms <- res.GetTerm()
				if res.GetVoteGranted() {
					votes.Add(1)
				}
			}()
		}
		wg.Wait()
		cancel()
		close(resTerms)
		gotHigherTerm := false
		for t := range resTerms {
			if t > s.CurrentTerm {
				gotHigherTerm = true
				s.CurrentTerm = t
			}
		}
		s.electionTickerDuration = randomDuration()
		s.electionTicker.Reset(s.electionTickerDuration)

		if gotHigherTerm {
			s.switchToFollower()
		} else if votes.Load() >= int32(majority) {
			s.logger.Info("i am a leader", "id", s.id)
			s.indexMu.Lock()
			s.switchToLeader()
			s.indexMu.Unlock()
		}
		s.mu.Unlock()
	}
}

func (s *Server) RequestVote(c context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Since(s.lastHeartBeat) <= heartbeatTickerDuration {
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
	}

	if req.Term < s.CurrentTerm {
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
	}

	if req.Term == s.CurrentTerm && s.VotedFor != "" {
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: s.VotedFor == req.CandidateId}, nil
	}

	s.switchToFollower()
	s.CurrentTerm = req.Term
	s.Persist()

	if (s.VotedFor == "" || s.VotedFor == req.CandidateId) && req.LastLogIndex >= s.lastLogIndex {
		s.VotedFor = req.CandidateId
		s.electionTickerDuration = randomDuration()
		s.electionTicker.Reset(s.electionTickerDuration)
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: true}, nil
	}

	return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
}

func (s *Server) applyClusterChange(_ context.Context, req *AppendEntriesRequest) (bool, error) {
	amIKickedOut := false
	isDisabled := false
	for _, r := range req.Entries {
		change := &ClusterChange{}
		err := json.Unmarshal(r, change)
		if err != nil {
			s.logger.Error(err.Error())
			return false, status.Error(codes.InvalidArgument, err.Error())
		}
		isDisabled = isDisabled || change.Disabled
		if isDisabled {
			s.switchToDisabled()
		}
		if !amIKickedOut {
			amIKickedOut = !slices.ContainsFunc(change.Cluster, func(p *ClusterChange_Peer) bool { return p.Id == s.id })
		}
		change.Cluster = slices.DeleteFunc(change.Cluster, func(p *ClusterChange_Peer) bool { return p.Id == s.id })
		s.Peers.Update(change)
	}
	s.Persist()
	if !isDisabled && s.role.Load() == disabled {
		s.switchToFollower()
	}
	return amIKickedOut, nil
}

func (s *Server) persistEntries(_ context.Context, req *AppendEntriesRequest) {
	entries := make([]Entry, 0, len(req.Entries))
	for i, e := range req.Entries {
		entries = append(entries, Entry{
			Term:  s.CurrentTerm,
			Index: s.lastLogIndex + uint64(i) + 1,
			Type:  req.GetType(),
			Data:  e,
		})
	}

	err := s.log.Append(entries)
	if err != nil {
		panic(err)
	}

	s.lastLogIndex += uint64(len(req.Entries))
	s.lastLogTerm = req.Term
}

func (s *Server) AppendEntries(c context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	}

	s.lastHeartBeat = time.Now()

	if req.Term > s.CurrentTerm {
		s.switchToFollower()
		s.CurrentTerm = req.Term
		s.Persist()
	}

	if req.PrevLogIndex > s.lastLogIndex {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	}

	if len(req.Entries) == 0 {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
	}

	if req.PrevLogTerm != s.lastLogTerm && req.PrevLogIndex < s.lastLogIndex {
		s.log.DeleteLastNEntries(s.lastLogIndex - req.PrevLogIndex)
	}

	if req.Type == AppendEntriesRequest_ClusterChange {
		amIKickedOut, err := s.applyClusterChange(c, req)
		if err != nil {
			return nil, err
		}
		if amIKickedOut {
			s.logger.Info("kicked out", "id", s.id, "role", s.role.Load())
		}
	}

	s.persistEntries(c, req)

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, s.lastLogIndex)
	}

	return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
}

func (s *Server) LeaderAppendEntries(c context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if s.role.Load() != leader {
		// TODO: return leader id
		return nil, status.Error(codes.Aborted, "")
	}

	if len(req.Entries) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty entries")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	amIKickedOut := false
	var err error
	if req.Type == AppendEntriesRequest_ClusterChange {
		amIKickedOut, err = s.applyClusterChange(c, req)
		if err != nil {
			return nil, err
		}
	}

	prevLogTerm := s.lastLogTerm
	s.persistEntries(c, req)

	wg := sync.WaitGroup{}
	wg.Add(len(s.Peers))
	c, cancel := context.WithTimeout(c, time.Duration(300)*time.Millisecond)
	defer cancel()
	resTerms := make(chan uint64, len(s.Peers))
	for pid, peer := range s.Peers {
		go func() {
			defer wg.Done()
			s.indexMu.Lock()
			nextIndex := s.nextIndex[pid]
			s.indexMu.Unlock()
			for retry := true; retry; {
				nEntries := s.log.LastNEntries(s.lastLogIndex - nextIndex - 1)
				req := &AppendEntriesRequest{
					Term:         s.CurrentTerm,
					LeaderId:     s.id,
					PrevLogIndex: nextIndex,
					LeaderCommit: s.commitIndex,
					PrevLogTerm:  prevLogTerm,
					Type:         req.Type,
				}
				for _, entry := range nEntries {
					req.Entries = append(req.Entries, entry.Data)
				}
				res, err := peer.client.AppendEntries(c, req)
				if err != nil {
					s.logger.Error(err.Error(), "pid", pid)
					return
				}
				resTerms <- res.GetTerm()
				s.indexMu.Lock()
				if res.GetSuccess() {
					s.nextIndex[pid] = s.lastLogIndex + 1
					s.matchIndex[pid] = s.lastLogIndex
					retry = false
				} else {
					s.nextIndex[pid] -= 1
				}
				s.indexMu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(resTerms)
	for t := range resTerms {
		if t > s.CurrentTerm {
			s.switchToFollower()
			s.CurrentTerm = t
			s.Persist()
			return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
		}
	}

	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	acks := 1 // count ourselves
	if amIKickedOut {
		acks = 0 // on cluster change, the current leader might get kicked out
	}
	for _, matchIndex := range s.matchIndex {
		if s.lastLogIndex > s.commitIndex && matchIndex >= s.lastLogIndex && s.lastLogTerm == s.CurrentTerm {
			acks++
		}
	}

	acked := acks >= (len(s.Peers)/2)+1
	if acked {
		s.commitIndex = s.lastLogIndex
	}

	if req.GetType() == AppendEntriesRequest_ClusterChange {
		// The second issue is that the cluster leader may not be
		// part of the new configuration. In this case, the leader steps
		// down (returns to follower state) once it has committed the
		// Cnew log entry. This means that there will be a period of
		// time (while it is committingCnew) when the leader is managing a cluster that does not include itself; it replicates log
		// entries but does not count itself in majorities. The leader
		// transition occurs when Cnew is committed because this is
		// the first point when the new configuration can operate independently (it will always be possible to choose a leader
		// from Cnew). Before this point, it may be the case that only
		// a server from Cold can be elected leader.
		s.switchToFollower()
	}

	return &AppendEntriesResponse{Term: s.CurrentTerm, Success: acked}, nil
}
