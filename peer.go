package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	Id     string `json:"id"`
	Target string `json:"target"`
	conn   *grpc.ClientConn
	client RaftClient
}
type Peers map[string]*Peer

func (ps Peers) Init() {
	for pid, p := range ps {
		pc, _ := grpc.NewClient(p.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		ps[pid].conn = pc
		ps[pid].client = NewRaftClient(pc)
	}
}

func (p Peers) Update(change *ClusterChange) {
	for _, peer := range p {
		peer.conn.Close()
	}
	clear(p)

	for _, peer := range change.GetCluster() {
		pc, _ := grpc.NewClient(peer.Target)
		p[peer.Id] = &Peer{
			Id:     peer.Id,
			Target: peer.Target,
			conn:   pc,
			client: NewRaftClient(pc),
		}
	}
}
