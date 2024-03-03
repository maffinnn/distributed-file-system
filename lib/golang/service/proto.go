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
	IsDir           bool
	FilePath        string
	ChildrenPaths   string
	LastModified    int64
	CallbackPromise bool
}

type UnmountRequest struct {
	ClientId string
	FilePath string
}

type UnmountResponse struct {
	IsSuccess bool
}

type CreateRequest struct {
	ClientId string
	FilePath string
}

type CreateResponse struct {
	IsSuccess    bool
	LastModified int64
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

// client side update callback
type UpdateCallbackPromiseRequest struct {
	FilePath          string
	IsValidOrCanceled bool
}

type UpdateCallbackPromiseResponse struct {
	IsSuccess bool
}

type GetAttributeRequest struct {
	ClientId string
	FilePath string
}

type GetAttributeResponse struct {
	IsDir        bool
	FilePath     string
	LastModified int64
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
	rpc.RegisterType(GetAttributeRequest{})
	rpc.RegisterType(GetAttributeResponse{})
	rpc.RegisterType(UpdateCallbackPromiseRequest{})
	rpc.RegisterType(UpdateCallbackPromiseResponse{})
	rpc.RegisterType(struct{}{})
}
