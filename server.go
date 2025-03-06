package main

import (
	"context"
	"sync"
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

	if req.Term < s.CurrentTerm {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
	}

	if req.Term > s.CurrentTerm {
		s.role = follower
		s.CurrentTerm = req.Term
		s.Persist()
	}

	prevEntry := s.log.LastEntry()

	if req.PrevLogIndex != prevEntry.Index || req.PrevLogTerm != prevEntry.Term {
		return &AppendEntriesResponse{Term: s.CurrentTerm, Success: false}, nil
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

	s.log.Append(entries)

	return &AppendEntriesResponse{Term: s.CurrentTerm, Success: true}, nil
}
