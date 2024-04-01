package main

import (
	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
	"flag"
)

func main() {
	addr := flag.String("addr", ":8080", "server address")
	networkPacketLossProbability := flag.Int("networkLossProb", 0, "")
	flag.Parse()
	rpc.ServerSideNetworkPacketLossProbability = *networkPacketLossProbability
	server := service.NewFileServer(*addr)
	server.Run()
}
