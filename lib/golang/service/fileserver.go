package service

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
	"distributed-file-system/lib/golang/service/proto"
)

type FileServer struct {
	addr string
	root string
	// exported []*file.FileDescriptor
	// accessList []*FileClient
}

func (fs *FileServer) find(path string) (string, error) {
	var localPath string
	err := filepath.Walk(fs.root, func(currentPath string, info os.FileInfo, err error) error {
		if strings.HasSuffix(currentPath, path) {
			localPath, err = filepath.Abs(currentPath)
			return err
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("file server: %v", err)
	} else if localPath == "" {
		return "", os.ErrNotExist
	}
	return localPath, nil
}

func (fs *FileServer) LookUp(req proto.LookUpRequest, resp *proto.LookUpResponse) error {
	log.Printf("FileServer.LookUp is called")
	localPath, err := fs.find(req.Src)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	fd := &file.FileDescriptor{
		Name: filepath.Base(localPath),
		Dir:  filepath.Dir(localPath),
	}
	resp.Fd = fd
	return nil
}

func (fs *FileServer) Create(req proto.CreateRequest, resp *proto.CreateResponse) error {
	log.Printf("FileServer.Create is called")
	// check if the file exists in the local env
	localPath, err := fs.find(filepath.Join(req.Dir, req.FileName))
	if localPath != "" || err != os.ErrNotExist {
		return fmt.Errorf("file server: %s already exists", localPath)
	}
	err = os.MkdirAll(filepath.Join(fs.root, req.Dir), os.ModePerm)
	if err != nil {
		return fmt.Errorf("file server: create error %v", err)
	}
	_, err = os.Create(filepath.Join(fs.root, req.Dir, req.FileName))
	if err != nil {
		return fmt.Errorf("file server: create error %v", err)
	}
	resp.Fd = &file.FileDescriptor{
		Name: filepath.Base(localPath),
		Dir:  filepath.Dir(localPath),
	}
	return nil
}

func (fs *FileServer) Read(req proto.ReadRequest, resp *proto.ReadResponse) error {
	log.Printf("FileServer.Create is called")
	localPath, err := fs.find(filepath.Join(req.Dir, req.FileName))
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	defer f.Close()
	buf := make([]byte, req.N)
	if _, err = f.ReadAt(buf, req.Offset); err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	resp.Content = append(resp.Content, buf...)
	return nil
}

func (fs *FileServer) Remove(req proto.RemoveRequest, resp *proto.RemoveResponse) error {
	log.Printf("FileServer.Remove is called")

	localPath, err := fs.find(filepath.Join(req.Dir, req.FileName))
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	if err = os.Remove(localPath); err != nil {
		resp.IsRemoved = false
		return fmt.Errorf("file server: %v", err)
	}
	resp.IsRemoved = true
	return nil
}

func (fs *FileServer) Write(req proto.WriteRequest, resp *proto.WriteResponse) error {
	log.Printf("FileServer.Write is called")
	localPath, err := fs.find(filepath.Join(req.Dir, req.FileName))
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	f1, err := os.Open(localPath)
	defer f1.Close()
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	buf1, buf2 := &bytes.Buffer{}, &bytes.Buffer{}
	if _, err = io.Copy(buf1, f1); err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	buf2.Write(buf1.Bytes()[:req.Offset])
	n, err := buf2.Write(req.Data)
	if err != nil {
		return fmt.Errorf("file server: write error %v", err)
	}
	buf2.Write(buf1.Bytes()[req.Offset:])
	f2, _ := os.Create(localPath)
	defer f2.Close()
	_, err = f2.Write(buf2.Bytes())
	if err != nil {
		return fmt.Errorf("file server: write error %v", err)
	}
	resp.N = int64(n)
	return nil
}

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
	fs := &FileServer{addr: config.ServerAddr, root: config.ServerRootLevel}
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
