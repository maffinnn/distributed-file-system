package proto

import (
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
)

type MountRequest struct {
	ClientId string
	File string
	Who  string // indicate which client is accessing
}

type MountResponse struct {
	Fd *file.FileDescriptor
}

type CreateRequest struct {
	ClientId string
	FilePath string
}

type CreateResponse struct {
	Fd *file.FileDescriptor
}

type ReadRequest struct {
	ClientId string
	FilePath	 string
	Offset   int64 // offset within the file
	N        int64 // number of bytes to read
}

type ReadResponse struct {
	Content []byte
}

type RemoveRequest struct {
	ClientId string
	FilePath string
}

type RemoveResponse struct {
	IsRemoved bool
}

type WriteRequest struct {
	ClientId string
	FilePath string
	Offset   int64
	Data     []byte
}

type WriteResponse struct {
	N int64 // number of bytes wrote
}


func init() {
	rpc.RegisterType(MountRequest{})
	rpc.RegisterType(MountResponse{})
	rpc.RegisterType(CreateRequest{})
	rpc.RegisterType(CreateResponse{})
	rpc.RegisterType(ReadRequest{})
	rpc.RegisterType(ReadResponse{})
	rpc.RegisterType(RemoveRequest{})
	rpc.RegisterType(RemoveResponse{})
	rpc.RegisterType(WriteRequest{})
	rpc.RegisterType(WriteResponse{})
	rpc.RegisterType(struct{}{})
}
