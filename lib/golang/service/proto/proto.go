package proto

import (
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
)

func init(){
	rpc.RegisterType(LookUpRequest{})
	rpc.RegisterType(LookUpResponse{})
	rpc.RegisterType(CreateRequest{})
	rpc.RegisterType(CreateResponse{})
	rpc.RegisterType(ReadRequest{})
	rpc.RegisterType(ReadResponse{})
	rpc.RegisterType(struct{}{})
}

type LookUpRequest struct {
	Src string
}

type LookUpResponse struct {
	Fd * file.FileDescriptor
}

type CreateRequest struct {
	Dir	 		string
	FileName	string
}

type CreateResponse struct {
	Fd *file.FileDescriptor
}

type ReadRequest struct {
	Dir 	 string
	FileName string
	Offset int64 // offset within the file
	N int64  // number of bytes to read
}

type ReadResponse struct {
	Content []byte
}