package service

import (
	"distributed-file-system/pkg/golang/rpc"
)

type MountRequest struct {
	FileSystemType string // indicating client's mount file system type, i.e. Andrew File System or Sun Network File System
	ClientId       string // indicating which client
	ClientAddr     string // the client network address
	FilePath       string // the file path that the client is going to mount
}

type MountResponse struct {
	IsDir           bool   //indicating if the requested file path is a directory
	FilePath        string // the relative file path at the client side
	ChildrenPaths   string // list of children of the requested file path, concatenated into a string
	LastModified    int64  // last modification time at the server side
	CallbackPromise bool   // callback promise used in Andrew File System; true means this callback promise is valid
}

type UnmountRequest struct {
	ClientId string
	FilePath string // the file path for unmounting
}

type UnmountResponse struct {
	IsSuccess bool
}

type CreateRequest struct {
	ClientId string
	FilePath string // file name to be created at the file server side
}

type CreateResponse struct {
	IsSuccess    bool
	LastModified int64 // sending back a last modified timestamp to client
}

type ReadRequest struct {
	ClientId string
	FilePath string
}

type ReadResponse struct {
	Data []byte // data for the read operation
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
	IsValidOrCanceled bool // true if valid
}

type UpdateCallbackPromiseResponse struct {
	IsSuccess bool
}

type GetAttributeRequest struct {
	ClientId string
	FilePath string // which file for getting the attribute
}

type GetAttributeResponse struct {
	IsDir        bool
	FilePath     string
	LastModified int64 // to synchronize the last modified timestamp at the server side
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
