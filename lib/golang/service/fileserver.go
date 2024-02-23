package service

import (
	"log"
	"os"
	"fmt"
	"net"
	"errors"
	"path/filepath"

	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/service/file"
	"distributed-file-system/lib/golang/service/proto"
)



type FileServer struct {
	addr string
	// exported []*file.FileDescriptor
	// accessList []*FileClient

}

func (fs *FileServer) LookUp(req proto.LookUpRequest, resp *proto.LookUpResponse) {
	filepath := req.Src
	fd, err := fs.lookUp(filepath)
	resp.Fd = fd
	resp.Err = err
}

func (fs *FileServer) lookUp(path string) (*file.FileDescriptor, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("file server: %s does not exist", path)
	}
	fd := &file.FileDescriptor{
		Name: filepath.Base(path),
		Dir: filepath.Dir(path),
	}
	return fd, nil
}

// func (fs *FileServer) Create(dirfh *file.FileDescriptor, name, attr string) (newfh *FileDescriptor) {
// 	return nil
// }

// func (fs *FileServer) Remove(dirfh *FileDescriptor, name string) (status string) {
// 	return ""
// }

// func (fs *FileServer) GetAttr(fh *FileDescriptor) (attr string) {
// 	return ""
// }

// func (fs *FileServer) Read(fh *FileDescriptor, offset, n int) (attr string, data []byte) {
// 	return "", nil
// }

// func (fs *FileServer) Write(fh *FileDescriptor, offset, n int, data []byte) (attr string) {
// 	return ""
// }

// func (fs *FileServer) Rename(dirfh *FileDescriptor, name string, todirfh *FileDescriptor, toname string) (status string) { 
// 	return ""
// }

// func (fs *FileServer) MakeDir(dirfh *FileDescriptor, name, attr string) (newfh *FileDescriptor) { 
// 	return nil
// }

// func (fs *FileServer) RemoveDir(dirfh *FileDescriptor, name string) (status string) { 
// 	return ""
// }


func NewFileServer(config *config.Config) *FileServer {
	fs := &FileServer{addr: config.ServerAddr}
	if err := rpc.Register(fs); err != nil {
		log.Fatal("rpc register error:", err)
	}
	return fs
}

func (fs *FileServer) Run() {
	s, err := net.ResolveUDPAddr("udp", fs.addr)
	if err != nil {
		log.Fatal("client error resolving udp address: ", err)
	}
	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on ", conn.LocalAddr().String())
	rpc.Accept(conn)
}

func (fs *FileServer) export() {

}