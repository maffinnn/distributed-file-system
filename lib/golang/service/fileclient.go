package service

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/rpc"
)

type FileClient struct {
	Id           string
	addr         string
	rpcClient    *rpc.Client
	rpcServer    *rpc.Server
	simplecache  map[string]*Entry          // filepath to the file content
	mountedFiles map[string]*FileDescriptor // translate local file path to server sIde file path
}

func NewFileClient(Id, addr string, config *config.Config) *FileClient {
	fc := &FileClient{
		Id:           Id,
		addr:         addr,
		simplecache:  make(map[string]*Entry),
		mountedFiles: make(map[string]*FileDescriptor),
	}
	fc.rpcServer = rpc.NewServer()
	if err := fc.rpcServer.Register(fc); err != nil {
		log.Fatal("rpc register error:", err)
		return nil
	}
	rpcClient, err := rpc.Dial(config.ServerAddr)
	if err != nil {
		log.Printf("file client rpc dial error: %v", err)
		return nil
	}
	fc.rpcClient = rpcClient
	return fc
}

func (fc *FileClient) Run() {
	s, err := net.ResolveUDPAddr("udp", fc.addr)
	if err != nil {
		log.Fatal("client error resolving udp address: ", err)
	}
	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Printf("[file client %s]: listening on %s", fc.Id, conn.LocalAddr().String())
	fc.rpcServer.Accept(conn)
}

func (fc *FileClient) Mount(src, target string, timeout int) {
	args := &MountRequest{FilePath: src, ClientId: fc.Id, ClientAddr: fc.addr}
	var reply MountResponse
	if err := fc.rpcClient.Call("FileServer.Mount", args, &reply); err != nil {
		log.Printf("call FileServer.Mount error: %v", err)
		return
	}
	log.Printf("[file client %s]: %s is mounted at %v", fc.Id, src, target)
	fc.mountedFiles[target] = reply.Fd
	fc.ListFiles(target)
	// timeout <= 0 means no timeout, the files will be mounted until client explicitly call unmount
	if timeout > 0 {
		go fc.monitor(src, target, timeout)
	}
}

func (fc *FileClient) monitor(src, target string, timeout int) {
	<-time.After(time.Duration(timeout) * time.Second)
	fc.Unmount(src, target)
	log.Printf("[file client %s]: timeout, unmounting file: %s", fc.Id, target)
}

func (fc *FileClient) Unmount(src, target string) {
	args := &UnmountRequest{FilePath: src, ClientId: fc.Id}
	var reply UnmountResponse
	if err := fc.rpcClient.Call("FileServer.Unmount", args, &reply); err != nil {
		log.Printf("call FileServer.Unmount error: %v", err)
		return
	}
	delete(fc.mountedFiles, target)
	fc.ListAllFiles()
}

// create a file
func (fc *FileClient) Create(localPath string) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localPath, mountPoint)
	args := &CreateRequest{FilePath: filepath.Join(fd.FilePath, suffix)}
	var reply CreateResponse
	if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
		log.Printf("call FileServer.Create error: %v", err)
		return
	}
	AddToTree(fd, reply.Fd)
}

func (fc *FileClient) MakeDir(dir string) {
	mountPoint, err := fc.checkMountingPoint(dir)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(dir, mountPoint)
	args := &CreateRequest{FilePath: filepath.Join(fd.FilePath, suffix)}
	var reply CreateResponse
	if err := fc.rpcClient.Call("FileServer.MkDir", args, &reply); err != nil {
		log.Printf("call FileServer.MkDir error: %v", err)
		return
	}
	AddToTree(fd, reply.Fd)
}

func (fc *FileClient) RemoveDir(dir string) {
	mountPoint, err := fc.checkMountingPoint(dir)
	if err != nil {
		log.Printf("[file client %s]: %v", fc.Id, err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(dir, mountPoint)
	args := &RemoveRequest{FilePath: filepath.Join(fd.FilePath, suffix)}
	var reply RemoveResponse
	if err := fc.rpcClient.Call("FileServer.RmDir", args, &reply); err != nil {
		log.Printf("call FileServer.RmDir error: %v", err)
		return
	}
	// remove the fd from the tree
	RemoveFromTree(fd, filepath.Join(fd.FilePath, suffix))
}

func (fc *FileClient) checkMountingPoint(file string) (string, error) {
	for mountPoint := range fc.mountedFiles {
		if strings.HasPrefix(file, mountPoint) {
			return mountPoint, nil
		}
	}
	return "", os.ErrNotExist
}

// Idempotent Read Operation:
// stateless read operation, does not change the seeker position of the file descriptor
// Note: provIded file must be a single file not a directory
func (fc *FileClient) ReadAt(localPath string, offset, n int) ([]byte, error) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return nil, err
	}
	rootFd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(rootFd, suffix)
	if fd.IsDir {
		return nil, fmt.Errorf("invalId read operation, %s is a directory", localPath)
	}

	// check cache
	if _, ok := fc.simplecache[fd.FilePath]; !ok {
		// not cached
		args := &ReadRequest{FilePath: fd.FilePath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return nil, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		fc.simplecache[fd.FilePath] = &Entry{b: bytes.NewBuffer(reply.Data)}
	}

	cached := fc.simplecache[fd.FilePath]
	if offset >= cached.Len() {
		return nil, fmt.Errorf("invalId read: offset exceeds the file length")
	}
	return cached.Bytes()[offset:min(offset+n, cached.Len())], nil
}

// Non-Idempotent Read: read from last seek position recorded at client sIde fd
// Note: provIded file must be a single file not a directory
func (fc *FileClient) Read(localPath string, n int) ([]byte, error) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return nil, err
	}
	rootFd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(rootFd, suffix)
	if fd.IsDir {
		return nil, fmt.Errorf("invalId read operation, %s is a directory", localPath)
	}
	// check cache
	if _, ok := fc.simplecache[fd.FilePath]; !ok {
		// not cached
		args := &ReadRequest{ClientId: fc.Id, FilePath: fd.FilePath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return nil, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		fc.simplecache[fd.FilePath] = &Entry{b: bytes.NewBuffer(reply.Data)}
	}
	cached := fc.simplecache[fd.FilePath]
	// update last read end position
	lastOffset := int(fd.Seeker)
	fd.Seeker = uint64(min(lastOffset+n, cached.Len()))
	return cached.Bytes()[lastOffset:int(fd.Seeker)], nil
}

func (fc *FileClient) Write(localPath string, offset int, data []byte) (int, error) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return 0, err
	}
	rootFd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(rootFd, suffix)
	if fd.IsDir {
		return 0, fmt.Errorf("invalId write operation, %s is a directory", localPath)
	}

	// update cached content
	// check cache
	if _, ok := fc.simplecache[fd.FilePath]; !ok {
		// not cached
		args := &ReadRequest{FilePath: fd.FilePath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return 0, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		fc.simplecache[fd.FilePath] = &Entry{b: bytes.NewBuffer(reply.Data)}
	}
	cached := fc.simplecache[fd.FilePath]
	if offset >= cached.Len() {
		return 0, fmt.Errorf("invalId write: offset exceeds the file length")
	}
	copy := &bytes.Buffer{}
	copy.Write(cached.Bytes())
	cached.Reset()
	cached.Write(copy.Bytes()[:offset])
	n, err := cached.Write(data)
	if err != nil {
		return n, err
	}
	cached.Write(copy.Bytes()[offset:])
	// evict cache to server
	args := &WriteRequest{ClientId: fc.Id, FilePath: fd.FilePath, Data: cached.Bytes()}
	var reply WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		return 0, fmt.Errorf("call FileServer.Write error: %v", err)
	}
	return n, nil
}

func (fc *FileClient) Remove(localfilepath string) {
	mountPoint, err := fc.checkMountingPoint(localfilepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localfilepath, mountPoint)
	args := &RemoveRequest{ClientId: fc.Id, FilePath: filepath.Join(fd.FilePath, suffix)}
	var reply RemoveResponse
	if err := fc.rpcClient.Call("FileServer.Remove", args, &reply); err != nil {
		log.Printf("call FileSever.Remove error: %v", err)
		return
	}
	log.Printf("reply: remove status %v", reply.IsRemoved)
	RemoveFromTree(fd, filepath.Join(fd.FilePath, suffix))
}

// client rpc
func (fc *FileClient) UpdateFile(req UpdateFileRequest, resp *UpdateFileResponse) error {
	log.Printf("[file client %s]: FileClient.UpdateFile is called", fc.Id)
	// perform the update only if the file exists in the cache
	if _, ok := fc.simplecache[req.FilePath]; !ok {
		log.Printf("[file client %s] %s is not cached: no-ops", fc.Id, req.FilePath)
		return nil
	}
	fc.simplecache[req.FilePath] = &Entry{b: bytes.NewBuffer(req.Data)}
	return nil
}

func (fc *FileClient) Shutdown() {
	fc.rpcServer.Shutdown()
}

// utility function for display purpose
func (fc *FileClient) ListAllFiles() {
	fmt.Printf("local file tree:\n")
	for root, fd := range fc.mountedFiles {
		PrintTree(root, fd)
	}
}

func (fc *FileClient) ListFiles(path string) {
	mountPoint, err := fc.checkMountingPoint(path)
	if err != nil {
		log.Printf("[file client %s]: %v", fc.Id, err)
		return
	}
	fmt.Printf("local file tree:\n")
	fd := fc.mountedFiles[mountPoint]
	PrintTree(path, fd)
}
