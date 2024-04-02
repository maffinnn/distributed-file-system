package config

import "time"

type Config struct {
	FilterDuplicatedRequest                bool
	ServerSideNetworkPacketLossProbability int
	ClientSideNetworkPacketLossProbability int
	RpcTimeout                             time.Duration
}
