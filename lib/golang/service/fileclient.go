package service

import (
	"log"
	"os"
	fp "path/filepath"
	"strings"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/file"
	"distributed-file-system/lib/golang/service/proto"
)

type FileClient struct {
	id           string
	rpcClient    *rpc.Client
	mountedFiles map[string]*file.FileDescriptor // translate local file path to server side file path
}

func NewFileClient(id string, config *config.Config) *FileClient {
	rpcClient, err := rpc.Dial(config.ServerAddr)
	if err != nil {
		log.Printf("file client rpc dial error: %v", err)
		return nil
	}
	return &FileClient{
		id:           id,
		rpcClient:    rpcClient,
		mountedFiles: make(map[string]*file.FileDescriptor),
	}
}

func (fc *FileClient) Run() {
	
}

// TODO: check permission when client is mounting
func (fc *FileClient) Mount(src, target, fstype string) {
	args := &proto.MountRequest{File: src, Who: fc.id}
	var reply proto.MountResponse
	if err := fc.rpcClient.Call("FileServer.Mount", args, &reply); err != nil {
		log.Printf("call FileServer.Mount error: %v", err)
		return
	}
	log.Printf("file client: %s is mounted at %v", src, target)
	fc.mountedFiles[target] = reply.Fd
	fc.PrintFiles(target)
}

func (fc *FileClient) PrintFiles(entry string) {
	mountPoint, err := fc.checkMountingPoint(entry)
	if err != nil {
		log.Printf("file client: %v", err)
	}
	fd := fc.mountedFiles[mountPoint]
	file.Print(entry, fd)
}

func (fc *FileClient) Unmount(target string) {
	delete(fc.mountedFiles, target)
}

func (fc *FileClient) Create(filepath string) {
	mountPoint, err := fc.checkMountingPoint(filepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(filepath, mountPoint)
	args := &proto.CreateRequest{FilePath: fp.Join(fd.FilePath, suffix)}
	var reply proto.CreateResponse
	if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
		log.Printf("call FileServer.Create error: %v", err)
		return
	}
	file.Add(fd, reply.Fd)
}

func (fc *FileClient) MakeDir(dir string) {
	mountPoint, err := fc.checkMountingPoint(dir)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(dir, mountPoint)
	args := &proto.CreateRequest{FilePath: fp.Join(fd.FilePath, suffix)}
	var reply proto.CreateResponse
	if err := fc.rpcClient.Call("FileServer.MkDir", args, &reply); err != nil {
		log.Printf("call FileServer.MkDir error: %v", err)
		return
	}
	file.Add(fd, reply.Fd)
}

func (fc *FileClient) RemoveDir(dir string) {
	mountPoint, err := fc.checkMountingPoint(dir)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	mountfd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(dir, mountPoint)
	args := &proto.RemoveRequest{FilePath: fp.Join(mountfd.FilePath, suffix)}
	var reply proto.RemoveResponse
	if err := fc.rpcClient.Call("FileServer.RmDir", args, &reply); err != nil {
		log.Printf("call FileServer.RmDir error: %v", err)
		return
	}
	// remove the fd from the tree
}

func (fc *FileClient) checkMountingPoint(file string) (string, error) {
	for mountPoint := range fc.mountedFiles {
		if strings.HasPrefix(file, mountPoint) {
			return mountPoint, nil
		}
	}
	return "", os.ErrNotExist
}

func (fc *FileClient) Read(filepath string, offset, n int64) {
	mountPoint, err := fc.checkMountingPoint(filepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(filepath, mountPoint)
	args := &proto.ReadRequest{FilePath: fp.Join(fd.FilePath, suffix), Offset: offset, N: n}
	var reply proto.ReadResponse
	if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
		log.Printf("call FileServer.Read error: %v", err)
		return
	}
	log.Printf("reply: %s", string(reply.Content))
}

func (fc *FileClient) Write(filepath string, offset int64, data []byte) {
	mountPoint, err := fc.checkMountingPoint(filepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(filepath, mountPoint)
	args := &proto.WriteRequest{FilePath: fp.Join(fd.FilePath, suffix), Offset: offset, Data: data}
	var reply proto.WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		log.Printf("call FileServer.Write error: %v", err)
		return
	}
	log.Printf("reply: %d bytes write to the file", reply.N)
}

func (fc *FileClient) Remove(filepath string) {
	mountPoint, err := fc.checkMountingPoint(filepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(filepath, mountPoint)
	args := &proto.RemoveRequest{FilePath: fp.Join(fd.FilePath, suffix)}
	var reply proto.RemoveResponse
	if err := fc.rpcClient.Call("FileServer.Remove", args, &reply); err != nil {
		log.Printf("call FileSever.Remove error: %v", err)
		return
	}
	log.Printf("reply: remove status %v", reply.IsRemoved)
	file.Remove(fd, fp.Join(fd.FilePath, suffix))
}
