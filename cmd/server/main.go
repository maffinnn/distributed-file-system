package main

import (
	"distributed-file-system/pkg/golang/config"
	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
	"flag"
	"fmt"
)

var serverAddr = ":8080"

func main() {
	s := flag.String("setting", "SimpleTest", "")
	flag.Parse()

	settings := map[string]config.Config{
		"SimpleTest": {
			FilterDuplicatedRequest:                false,
			ServerSideNetworkPacketLossProbability: 0,
		},
		"AtLeastOnceIdempotentRead": {
			FilterDuplicatedRequest:                false,
			ServerSideNetworkPacketLossProbability: 50,
		},
		"AtLeastOnceNonIdempotentRead": {
			FilterDuplicatedRequest:                false,
			ServerSideNetworkPacketLossProbability: 99,
		},
		"AtMostOnceIdempotentRead": {
			FilterDuplicatedRequest:                true,
			ServerSideNetworkPacketLossProbability: 50,
		},
		"AtMostOnceNonIdempotentRead": {
			FilterDuplicatedRequest:                true,
			ServerSideNetworkPacketLossProbability: 99,
		},
		"TestCacheConsistency": {
			FilterDuplicatedRequest:                true,
			ServerSideNetworkPacketLossProbability: 0,
		},
	}

	if conf, ok := settings[*s]; ok {
		rpc.ServerSideNetworkPacketLossProbability = conf.ServerSideNetworkPacketLossProbability
		server := service.NewFileServer(serverAddr)
		server.Run()
	} else {
		fmt.Printf("error flag")
		return
	}
}
