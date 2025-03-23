package main

import (
	"context"

	"google.golang.org/grpc"
)

type Peer struct {
	Id         string `json:"id"`
	Target     string `json:"target"`
	c          context.Context
	cancel     context.CancelFunc
	nextIndex  uint64
	matchIndex uint64
	conn       *grpc.ClientConn
	client     RaftClient
}
type Peers map[string]*Peer

func (ps Peers) Init() {
	for pid, p := range ps {
		ps[pid].c, ps[pid].cancel = context.WithCancel(context.Background())
		pc, _ := grpc.NewClient(p.Target)
		ps[pid].conn = pc
		ps[pid].client = NewRaftClient(pc)
	}
}

func (p Peers) Add(peers []*ClusterChange_Peer) {
	pids := make([]string, len(peers))
	for i, np := range peers {
		pids[i] = np.Id
	}
	p.Remove(pids)
	for _, peer := range peers {
		c, cancel := context.WithCancel(context.Background())
		pc, _ := grpc.NewClient(peer.Target)
		p[peer.Id] = &Peer{
			Id:     peer.Id,
			Target: peer.Target,
			c:      c,
			cancel: cancel,
			conn:   pc,
			client: NewRaftClient(pc),
		}
	}
}

func (p Peers) Remove(peers []string) {
	for _, peer := range peers {
		pc, ok := p[peer]
		if ok {
			pc.cancel()
			pc.conn.Close()
			delete(p, peer)
		}
	}
}
