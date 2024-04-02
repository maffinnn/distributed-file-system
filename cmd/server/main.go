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
	s := flag.String("setting", "AtLeastOnceIdempotent", "")
	flag.Parse()

	settings := map[string]config.Config{
		"AtLeastOnceIdempotent": {
			FilterDuplicatedRequest:                false,
			ServerSideNetworkPacketLossProbability: 20,
		},
		"AtLeastOnceNonIdempotent": {
			FilterDuplicatedRequest:                false,
			ServerSideNetworkPacketLossProbability: 50,
		},
		"AtMostOnceIdempotent": {
			FilterDuplicatedRequest:                true,
			ServerSideNetworkPacketLossProbability: 10,
		},
		"AtMostOnceNonIdempotentRead": {
			FilterDuplicatedRequest:                true,
			ServerSideNetworkPacketLossProbability: 50,
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
