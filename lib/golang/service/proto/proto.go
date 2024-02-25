package proto

import (
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
)

func init(){
	rpc.RegisterType(LookUpRequest{})
	rpc.RegisterType(LookUpResponse{})
	rpc.RegisterType(struct{}{})
}

type LookUpRequest struct {
	Src string
}

type LookUpResponse struct {
	// Err 	string // application level error
	Fd * file.FileDescriptor

}
