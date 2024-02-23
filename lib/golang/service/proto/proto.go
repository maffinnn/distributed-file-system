package proto

import (
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
)

func init(){
	rpc.Register(LookUpRequest{})
	rpc.Register(LookUpResponse{})
}

type LookUpRequest struct {
	Src string
}

type LookUpResponse struct {
	Err 	error // application level error
	Fd * file.FileDescriptor

}
