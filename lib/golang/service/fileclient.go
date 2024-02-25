package service

import (
	"log"
	// "strings"

	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/service/file"
	"distributed-file-system/lib/golang/service/proto"
)

type FileClient struct {
	rpcClient *rpc.Client
	local2remote map[string]string // translate local file path to server side file path
	openFiles []*file.FileDescriptor
}

func NewFileClient(config *config.Config) *FileClient {
	rpcClient, err := rpc.Dial(config.ServerAddr)
	if err != nil {
		log.Printf("file client rpc dial error: %v", err)
		return nil
	}
	return &FileClient{
		rpcClient: rpcClient,
		local2remote: make(map[string]string),
	}
}

func (fc *FileClient) Run() {
	
}

// TODO: check authorization when client is mounting
func (fc *FileClient) Mount(src, target, fstype string) {
	args := &proto.LookUpRequest{Src: src}
	var reply proto.LookUpResponse
	if err := fc.rpcClient.Call("FileServer.LookUp", args, &reply); err != nil {
		log.Printf("call FileServer.LookUp error:", err)
		return
	}
	log.Printf("reply.Fd: %v", reply.Fd)
	fc.local2remote[target] = src
}

func (fc *FileClient) Unmount(target string) {
	delete(fc.local2remote, target)
}

func (fc *FileClient) Open(path string){
	
}

func (fc *FileClient) Create(dir, name string) {
	// assume it's already exists in the map
	// srcDir := fc.local2remote[dir]
	args := &proto.CreateRequest{Dir: dir, FileName: name}
	var reply proto.CreateResponse
	if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
		log.Printf("call FileServer.Create error:", err)
		return
	}
	log.Printf("reply.Fd: %v", reply)
	fc.openFiles = append(fc.openFiles, reply.Fd)
}

func (fc *FileClient) Read(dir, name string, offset, n int64) {
	// srcDir := fc.local2remote[dir]
	args := &proto.ReadRequest{Dir: dir, FileName: name, Offset: offset, N: n}
	var reply proto.ReadResponse
	if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
		log.Printf("call FileServer.Read error:", err)
		return
	}
	log.Printf("reply: %s", string(reply.Content))
}