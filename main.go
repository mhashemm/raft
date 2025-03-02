package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
)

func main() {
	s := &Server{}
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", os.Getenv("GRPC_PORT")))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterRaftServer(grpcServer, s)
	grpcServer.Serve(lis)
}
