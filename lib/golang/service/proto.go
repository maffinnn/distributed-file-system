package service

import (
	"distributed-file-system/lib/golang/rpc"
)

type MountRequest struct {
	ClientId   string
	ClientAddr string
	FilePath   string
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
	FilePath string
}

type ReadResponse struct {
	Data []byte
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
	Data     []byte
}

type WriteResponse struct {
	N int64 // number of bytes wrote
}

type UpdateFileRequest struct {
	FilePath  string
	IsRemoved bool
	Data      []byte
}

type UpdateFileResponse struct {
	IsSuccess bool
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
	rpc.RegisterType(UpdateFileRequest{})
	rpc.RegisterType(UpdateFileResponse{})
	rpc.RegisterType(struct{}{})
}
