package service

import (
	"log"

	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/service/proto"
)

type FileClient struct {
}

func NewFileClient(config *config.Config) *FileClient {
	return &FileClient{}
}


func (fc *FileClient) Mount(serverAddr, src, target, fstype string) {
	rpcClient, err := rpc.Dial(serverAddr)
	if err != nil {
		log.Printf("file client rpc dial error: %v", err)
		return
	}
	args := &proto.LookUpRequest{Src: src}
	var reply proto.LookUpResponse
	if err := rpcClient.Call("FileServer.LookUp", args, &reply); err != nil {
		log.Fatal("call FileServer.LookUp error:", err)
	}
	
}


func (fc *FileClient) Unmount(target string) error {
	return nil
}
