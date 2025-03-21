package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Role byte

const (
	follower Role = iota
	leader
)

const (
	heartbeatTickerDuration = 100 * time.Millisecond
)

type Server struct {
	State
	mu                     sync.Mutex
	log                    Log
	commitIndex            uint64
	lastApplied            uint64
	lastLogIndex           uint64
	lastLogTerm            uint64
	nextIndex              map[string]uint64
	matchIndex             map[string]uint64
	role                   Role
	peers                  Peers
	logger                 *slog.Logger
	electionTickerDuration time.Duration
	electionTicker         *time.Ticker
	heartbeatTicker        *time.Ticker
	lastHeartBeat          time.Time
}

func randomDuration() time.Duration {
	return time.Duration((rand.IntN(150) + 150)) * time.Millisecond
}

func NewServer() *Server {
	s := State{}
	s.Init()
	l := Log{}
	l.Open()

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
		peers:                  Peers{},
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

func (s *Server) hearbeatWorker() {
	for range s.heartbeatTicker.C {
		if time.Since(s.lastHeartBeat).Abs() < heartbeatTickerDuration {
			continue
		}
		s.mu.Lock()
		wg := sync.WaitGroup{}
		wg.Add(len(s.peers))
		c, cancel := context.WithTimeout(context.TODO(), time.Duration(300)*time.Millisecond)
		for pid, peer := range s.peers {
			go func() {
				defer wg.Done()
				req := &AppendEntriesRequest{
					Term:         s.CurrentTerm,
					LeaderId:     s.Id,
					PrevLogIndex: s.lastLogIndex,
					LeaderCommit: s.commitIndex,
					PrevLogTerm:  s.lastLogTerm,
				}
				_, err := peer.client.AppendEntries(c, req)
				if err != nil {
					s.logger.Error("%s [pid=%s]", err.Error(), pid)
					return
				}
			}()
		}
		wg.Wait()
		cancel()
		s.lastHeartBeat = time.Now()
		s.mu.Unlock()
	}
}

func (s *Server) electionWorker() {
	for range s.electionTicker.C {
		if time.Since(s.lastHeartBeat) < s.electionTickerDuration {
			continue
		}
		s.mu.Lock()
		s.CurrentTerm += 1
		s.VotedFor = s.Id
		wg := sync.WaitGroup{}
		wg.Add(len(s.peers))
		c, cancel := context.WithTimeout(context.TODO(), time.Duration(300)*time.Millisecond)
		majorty := (len(s.peers) / 2) + 1
		votes := atomic.Int32{}
		resTerms := make(chan uint64, len(s.peers))
		for pid, peer := range s.peers {
			go func() {
				defer wg.Done()
				req := &RequestVoteRequest{
					Term:         s.CurrentTerm,
					CandidateId:  s.Id,
					LastLogIndex: s.lastLogIndex,
					LastLogTerm:  s.lastLogTerm,
				}
				res, err := peer.client.RequestVote(c, req)
				if err != nil {
					s.logger.Error("%s [pid=%s]", err.Error(), pid)
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
			s.role = follower
			s.heartbeatTicker.Stop()
		} else if votes.Load() >= int32(majorty) {
			s.role = leader
			s.heartbeatTicker.Reset(heartbeatTickerDuration)
			// TODO: reinit the rest
		}
		s.mu.Unlock()
	}
}

func (s *Server) RequestVote(c context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
	}

	if req.Term == s.CurrentTerm && s.VotedFor != "" {
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: s.VotedFor == req.CandidateId}, nil
	}

	s.role = follower
	s.heartbeatTicker.Stop()
	s.CurrentTerm = req.Term
	s.Persist()

	if (s.VotedFor == "" || s.VotedFor == req.CandidateId) && req.LastLogIndex >= s.matchIndex[s.Id] {
		s.VotedFor = req.CandidateId
		s.electionTickerDuration = randomDuration()
		s.electionTicker.Reset(s.electionTickerDuration)
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: true}, nil
	}

	return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
}

func (s *Server) AppendEntries(c context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.CurrentTerm {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	}

	s.lastHeartBeat = time.Now()

	if req.Term > s.CurrentTerm {
		s.heartbeatTicker.Stop()
		s.role = follower
		s.CurrentTerm = req.Term
		s.Persist()
	}

	if req.PrevLogIndex != s.lastLogIndex || req.PrevLogTerm != s.lastLogTerm {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	}

	if len(req.Entries) == 0 {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
	}

	if req.Type == AppendEntriesRequest_ClusterChange {
		for _, r := range req.Entries {
			change := &ClusterChange{}
			err := json.Unmarshal(r, change)
			if err != nil {
				s.logger.Error(err.Error())
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			err = s.peers.Remove(change.Remove)
			if err != nil {
				s.logger.Error(err.Error())
				return nil, status.Error(codes.Internal, err.Error())
			}
			err = s.peers.Add(change.Add)
			if err != nil {
				s.logger.Error(err.Error())
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	}

	prevLogIndex, prevLogTerm := s.lastLogIndex, s.lastLogTerm

	entries := make([]Entry, 0, len(req.Entries))
	for i, e := range req.Entries {
		entries = append(entries, Entry{
			Term:  s.CurrentTerm,
			Index: s.lastLogIndex + uint64(i) + 1,
			Data:  e,
		})
		s.nextIndex[s.Id] += 1
	}

	err := s.log.Append(entries)
	if err != nil {
		panic(err)
	}

	s.lastLogIndex += uint64(len(req.Entries))
	s.lastLogTerm = req.Term

	if s.role == follower {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
	}

	wg := sync.WaitGroup{}
	wg.Add(len(s.peers))
	c, cancel := context.WithTimeout(c, time.Duration(300)*time.Millisecond)
	defer cancel()
	majorty := (len(s.peers) / 2) + 1
	acks := atomic.Int32{}
	for pid, peer := range s.peers {
		go func() {
			defer wg.Done()
			req := &AppendEntriesRequest{
				Term:         s.CurrentTerm,
				LeaderId:     s.Id,
				PrevLogIndex: prevLogIndex,
				LeaderCommit: s.commitIndex,
				PrevLogTerm:  prevLogTerm,
				Type:         req.Type,
				Entries:      req.Entries,
			}
			res, err := peer.client.AppendEntries(c, req)
			if err != nil {
				s.logger.Error("%s [pid=%s]", err.Error(), pid)
				return
			}
			if res.GetSuccess() {
				acks.Add(1)
			}
		}()
	}
	wg.Wait()

	if acks.Load() >= int32(majorty) {
		s.commitIndex = s.nextIndex[s.Id] - 1
	}

	return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
}
