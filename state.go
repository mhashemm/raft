package main

import (
	"encoding/json"
	"os"

	"github.com/google/uuid"
)

type State struct {
	Id          string `json:"id"`
	CurrentTerm uint64 `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	Peers       Peers  `json:"peers"`
}

func (s *State) Init() {
	oldState, err := os.ReadFile("state.json")
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	if len(oldState) > 0 {
		err = json.Unmarshal(oldState, s)
		if err != nil {
			panic(err)
		}
	}
	s.Id = os.Getenv("ID")
	if s.Id == "" {
		s.Id = uuid.NewString()
		s.Persist()
	}
	if len(s.Peers) == 0 {
		s.Peers = Peers{}
	}
	s.Peers.Init()
}

func (s *State) Persist() {
	newState, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	err = os.WriteFile("state.json", newState, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

// TODO: persist log
type Log struct {
	// f *os.File
	s []Entry
}

func (l *Log) Open() {
	// f, err := os.OpenFile("log", os.O_CREATE|os.O_RDWR, os.ModePerm)
	// if err != nil {
	// 	panic(err)
	// }
	// l.f = f
}

func (l *Log) Count() uint64 {
	// _, err := l.f.Seek(0, 0)
	// if err != nil {
	// 	panic(err)
	// }
	// scanner := bufio.NewScanner(l.f)
	// i := uint64(0)
	// for scanner.Scan() {
	// 	if len(bytes.TrimSpace(scanner.Bytes())) > 0 {
	// 		i++
	// 	}
	// }
	// return i
	return uint64(len(l.s))
}

func (l *Log) Append(entries []Entry) error {
	// _, err := l.f.Seek(0, 2)
	// if err != nil {
	// 	panic(err)
	// }
	// for _, e := range entries {
	// 	data, err := json.Marshal(e)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	_, err = l.f.Write(data)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	// err = l.f.Sync()
	// if err != nil {
	// 	panic(err)
	// }
	l.s = append(l.s, entries...)
	return nil
}

func (l *Log) LastEntry() Entry {
	if len(l.s) == 0 {
		return Entry{}
	}
	return l.s[len(l.s)-1]
}

type Entry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Data  []byte `json:"data"`
}
