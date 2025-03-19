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

type Server struct {
	State
	mu             sync.Mutex
	log            Log
	commitIndex    uint64
	lastApplied    uint64
	nextIndex      map[string]uint64
	matchIndex     map[string]uint64
	role           Role
	peers          Peers
	logger         *slog.Logger
	electionTicker *time.Ticker
	heartbeat      chan struct{}
}

func randomDuration() time.Duration {
	return time.Duration((rand.IntN(150) + 150)) * time.Millisecond
}

func NewServer() *Server {
	s := State{}
	s.Init()
	l := Log{}
	l.Open()

	lastLogIndex := l.Count()
	server := &Server{
		State: s,
		log:   l,
		nextIndex: map[string]uint64{
			s.Id: lastLogIndex + 1,
		},
		matchIndex: map[string]uint64{
			s.Id: lastLogIndex,
		},
		peers:          Peers{},
		logger:         slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true})),
		electionTicker: time.NewTicker(randomDuration()),
		heartbeat:      make(chan struct{}, 1),
	}
	go server.electionWorker()
	return server
}

func (s *Server) electionWorker() {
	for {
		select {
		case <-s.electionTicker.C:
			s.mu.Lock()
			defer s.mu.Unlock()
			lastEntry := s.log.LastEntry()
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
						LastLogIndex: lastEntry.Index,
						LastLogTerm:  lastEntry.Term,
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

			s.electionTicker.Stop()
			s.electionTicker = time.NewTicker(randomDuration())

			if gotHigherTerm {
				s.role = follower
				continue
			}

			if votes.Load() >= int32(majorty) {
				s.role = leader
				// TODO: reinit the rest
			}

		case <-s.heartbeat:
			continue
		}
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
	s.CurrentTerm = req.Term
	s.Persist()

	if (s.VotedFor == "" || s.VotedFor == req.CandidateId) && req.LastLogIndex >= s.matchIndex[s.Id] {
		s.VotedFor = req.CandidateId
		s.electionTicker.Stop()
		s.electionTicker = time.NewTicker(randomDuration())
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

	s.heartbeat <- struct{}{}

	if req.Term > s.CurrentTerm {
		s.role = follower
		s.CurrentTerm = req.Term
		s.Persist()
	}

	prevEntry := s.log.LastEntry()

	if req.PrevLogIndex != prevEntry.Index || req.PrevLogTerm != prevEntry.Term {
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

	entries := make([]Entry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, Entry{
			Term:  s.CurrentTerm,
			Index: s.nextIndex[s.Id],
			Data:  e,
		})
		s.nextIndex[s.Id] += 1
	}

	err := s.log.Append(entries)
	if err != nil {
		panic(err)
	}

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
				PrevLogIndex: prevEntry.Index,
				LeaderCommit: s.commitIndex,
				PrevLogTerm:  prevEntry.Term,
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
		//TODO: commit the log first
		s.commitIndex = s.nextIndex[s.Id] - 1
	}

	return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
}
