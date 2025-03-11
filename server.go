package main

import (
	"context"
	"encoding/json"
	"log/slog"
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
	mu          sync.Mutex
	log         Log
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	role        Role
	peers       Peers
	logger      *slog.Logger
	leaderId    string
}

func NewServer() *Server {
	s := State{}
	s.Init()
	l := Log{}
	l.Open()

	lastLogIndex := l.Count()
	return &Server{
		State: s,
		log:   l,
		nextIndex: map[string]uint64{
			s.Id: lastLogIndex + 1,
		},
		matchIndex: map[string]uint64{
			s.Id: lastLogIndex,
		},
		peers:  Peers{},
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true})),
	}
}

func (s *Server) RequestVote(c context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term > s.CurrentTerm {
		s.role = follower
		s.CurrentTerm = req.Term
		s.Persist()
	}

	if (s.VotedFor == "" || s.VotedFor == req.CandidateId) &&
		req.Term > s.CurrentTerm &&
		req.LastLogIndex >= s.matchIndex[s.Id] {
		return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: true}, nil
	}

	return &RequestVoteResponse{Term: s.CurrentTerm, VoteGranted: false}, nil
}

func (s *Server) AppendEntries(c context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.role == follower && req.LeaderId == "" {
		return nil, status.Error(codes.Aborted, s.leaderId)
	}

	if req.Term < s.CurrentTerm {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	}

	if req.Term > s.CurrentTerm {
		s.role = follower
		s.CurrentTerm = req.Term
		s.Persist()
	}

	if s.role == follower {
		s.leaderId = req.LeaderId
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
