package service

import (
	"distributed-file-system/lib/golang/config"
	"log"
	// "strings"

	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
	"distributed-file-system/lib/golang/service/proto"
)

type FileClient struct {
	rpcClient    *rpc.Client
	local2remote map[string]string // translate local file path to server side file path
	openFiles    []*file.FileDescriptor
}

func NewFileClient(config *config.Config) *FileClient {
	rpcClient, err := rpc.Dial(config.ServerAddr)
	if err != nil {
		log.Printf("file client rpc dial error: %v", err)
		return nil
	}
	return &FileClient{
		rpcClient:    rpcClient,
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
		log.Printf("call FileServer.LookUp error: %v", err)
		return
	}
	fc.local2remote[target] = src
}

func (fc *FileClient) Unmount(target string) {
	delete(fc.local2remote, target)
}

func (fc *FileClient) Open(path string) {

}

func (fc *FileClient) Create(dir, name string) {
	// assume it's already exists in the map
	// srcDir := fc.local2remote[dir]
	args := &proto.CreateRequest{Dir: dir, FileName: name}
	var reply proto.CreateResponse
	if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
		log.Printf("call FileServer.Create error: %v", err)
		return
	}
	fc.openFiles = append(fc.openFiles, reply.Fd)
}

func (fc *FileClient) Read(dir, name string, offset, n int64) {
	// srcDir := fc.local2remote[dir]
	args := &proto.ReadRequest{Dir: dir, FileName: name, Offset: offset, N: n}
	var reply proto.ReadResponse
	if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
		log.Printf("call FileServer.Read error: %v", err)
		return
	}
	log.Printf("reply: %s", string(reply.Content))
}

func (fc *FileClient) Write(dir, name string, offset int64, data []byte) {
	args := &proto.WriteRequest{Dir: dir, FileName: name, Offset: offset, Data: data}
	var reply proto.WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		log.Printf("call FileServer.Write error: %v", err)
		return
	}
	log.Printf("reply: %d bytes write to the file", reply.N)
}

func (fc *FileClient) Remove(dir, name string) {
	args := &proto.RemoveRequest{
		Dir:      dir,
		FileName: name,
	}
	var reply proto.RemoveResponse
	if err := fc.rpcClient.Call("FileServer.Remove", args, &reply); err != nil {
		log.Printf("call FileSever.Remove error: %v", err)
		return
	}
	log.Printf("reply: remove status %v", reply.IsRemoved)
}
