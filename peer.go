package main

import (
	"google.golang.org/grpc"
)

type Peer struct {
	conn   *grpc.ClientConn
	client RaftClient
}
type Peers map[string]*Peer

func (p Peers) Add(peers []string) error {
	p.Remove(peers)
	for _, peer := range peers {
		pc, err := grpc.NewClient(peer)
		if err != nil {
			return err
		}
		p[peer] = &Peer{
			conn:   pc,
			client: NewRaftClient(pc),
		}
	}
	return nil
}

func (p Peers) Remove(peers []string) error {
	for _, peer := range peers {
		pc, ok := p[peer]
		if ok {
			pc.conn.Close()
			delete(p, peer)
		}
	}
	return nil
}
