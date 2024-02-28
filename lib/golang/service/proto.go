package service

import (
	"distributed-file-system/lib/golang/rpc"
)

type MountRequest struct {
	ClientId 	string
	ClientAddr  string
	FilePath 	string
}

type MountResponse struct {
	Fd *FileDescriptor
}

type UnmountRequest struct {
	ClientId string
	FilePath string
}

type UnmountResponse struct {
	Success bool
}

type CreateRequest struct {
	ClientId string
	FilePath string
}

type CreateResponse struct {
	Fd *FileDescriptor
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

type UpdateRequest struct {

}

type UpdateResponse struct {

}


func init() {
	rpc.RegisterType(MountRequest{})
	rpc.RegisterType(MountResponse{})
	rpc.RegisterType(UnmountRequest{})
	rpc.RegisterType(UnmountResponse{})
	rpc.RegisterType(CreateRequest{})
	rpc.RegisterType(CreateResponse{})
	rpc.RegisterType(ReadRequest{})
	rpc.RegisterType(ReadResponse{})
	rpc.RegisterType(RemoveRequest{})
	rpc.RegisterType(RemoveResponse{})
	rpc.RegisterType(WriteRequest{})
	rpc.RegisterType(WriteResponse{})
	rpc.RegisterType(UpdateRequest{})
	rpc.RegisterType(UpdateResponse{})
	rpc.RegisterType(struct{}{})
}
