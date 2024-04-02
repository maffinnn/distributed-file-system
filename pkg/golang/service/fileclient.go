package service

import (
	"bytes"
	"fmt"
	"net"
	"os"
	fp "path/filepath"
	"strings"
	"sync"
	"time"

	"distributed-file-system/pkg/golang/logger"
	"distributed-file-system/pkg/golang/rpc"
)

var (
	Duration     int = 0   // in seconds, default will mount the volume forever
	PollInterval int = 100 // in miliseconds
)

type FileClient struct {
	id        string
	addr      string
	rpcClient *rpc.Client
	rpcServer *rpc.Server
	stop      chan struct{}
	volumes   map[string]*Volume // file index for mounted files
	cache     *Cache
	logger    *logger.Logger
}

func NewFileClient(id, addr, serverAddr string) *FileClient {
	logger := logger.NewLogger(fmt.Sprintf("./client%s.log", id))
	fc := &FileClient{
		id:        id,
		addr:      addr,
		stop:      make(chan struct{}),
		volumes:   make(map[string]*Volume),
		cache:     NewCache(),
		logger:    logger,
		rpcServer: rpc.NewServer(logger),
	}
	if err := fc.rpcServer.Register(fc); err != nil {
		panic(fmt.Sprintf("file client rpc register error: %v", err))
	}
	rpcClient, err := rpc.Dial(serverAddr, logger)
	if err != nil {
		panic(fmt.Sprintf("file client rpc dial error: %v", err))
	}
	fc.rpcClient = rpcClient
	return fc
}

func (fc *FileClient) Run() {
	s, err := net.ResolveUDPAddr("udp", fc.addr)
	if err != nil {
		panic(fmt.Sprintf("client error resolving udp address: %v", err))
	}
	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		panic(fmt.Sprintf("network error: %v", err))
	}
	fc.logger.Printf("INFO [file client %s]: listening on %s", fc.id, conn.LocalAddr().String())
	fc.rpcServer.Accept(conn)
}

// user facing method
// recurisively mount the `src` directory on the server side to the `target` location at the client side with specified file system type
func (fc *FileClient) Mount(src, target string, fstype FileSystemType) error {
	args := &MountRequest{FilePath: src}
	args.ClientId = fc.id
	args.ClientAddr = fc.addr
	args.FileSystemType = string(fstype)
	var reply MountResponse
	if err := fc.rpcClient.Call("FileServer.Mount", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileServer.Mount error: %v", fc.id, err)
	}
	fc.logger.Printf("INFO [file client %s]: %s is mounted at %v", fc.id, src, target)
	root := NewFileDescriptor(reply.IsDir, reply.FilePath, uint64(reply.Size))
	childrenPaths := strings.Split(reply.ChildrenPaths, ":")
	for _, cp := range childrenPaths {
		fc.mountRecursive(root, cp, fstype)
	}
	fc.volumes[target] = NewVolume(root, fstype)
	fc.ListFiles(target)
	// NFS requires polling at the client side
	if fstype == SunNetworkFileSystemType {
		go fc.poll()
	}
	// duration <= 0 means mount forever, the files will be mounted until client explicitly call unmount
	if Duration > 0 {
		go fc.monitor(src, target)
	}
	return nil
}

func (fc *FileClient) mountRecursive(root *FileDescriptor, filepath string, fstype FileSystemType) {
	if root == nil || filepath == "" {
		return
	}
	args := &MountRequest{FilePath: filepath}
	if fstype == AndrewFileSystemType {
		args.ClientId = fc.id
		args.ClientAddr = fc.addr
	}
	var reply MountResponse
	_ = fc.rpcClient.Call("FileServer.Mount", args, &reply)
	fd := NewFileDescriptor(reply.IsDir, reply.FilePath, uint64(reply.Size))
	root.Children = append(root.Children, fd)
	childrenPaths := strings.Split(reply.ChildrenPaths, ":")
	for _, cp := range childrenPaths {
		fc.mountRecursive(fd, cp, fstype)
	}
}

func (fc *FileClient) monitor(src, target string) error {
	<-time.After(time.Duration(Duration) * time.Second)
	fc.stop <- struct{}{}
	fc.logger.Printf("INFO [file client %s]: timeout, unmounting file: %s", fc.id, target)
	return fc.unmount(src, target)
}

func (fc *FileClient) poll() {
	freshnessPeriod := time.Duration(PollInterval) * time.Millisecond
	for {
		select {
		case <-fc.stop:
			return
		default:
			// check for all cached(open) file
			var wg sync.WaitGroup
			now := time.Now()
			for filepath, entry := range fc.cache.cc {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if now.Sub(entry.lastValidated) < freshnessPeriod {
						return // consider valid
					}
					getArgs := &GetAttributeRequest{ClientId: fc.id, FilePath: filepath}
					var getReply GetAttributeResponse
					if err := fc.rpcClient.Call("FileServer.GetAttribute", getArgs, &getReply); err != nil {
						fc.logger.Printf("ERROR [file client %s]: call FileServer.GetAttribute error: %v", fc.id, err)
						return
					}
					_, fd, _ := fc.find(filepath)
					lastModifiedAtServer := getReply.LastModified
					if lastModifiedAtServer == fd.LastModified {
						// no change at the server, update the lastValidated timestamp
						entry.lastValidated = now
						return
					}
					// invalidated the entry
					readArgs := &ReadRequest{FilePath: fd.Filepath}
					var readReply ReadResponse
					if err := fc.rpcClient.Call("FileServer.Read", readArgs, &readReply); err != nil {
						fc.logger.Printf("ERROR [file client %s]: call FileServer.Read error: %v", fc.id, err)
						return
					}
					entry.Reset()
					entry.Write(readReply.Data)
					entry.lastValidated = now
					entry.dirty = false
				}()
			}
			wg.Wait()
		}
	}
}

// user facing method
// ummoun the specified `target` file path
func (fc *FileClient) unmount(src, target string) error {
	args := &UnmountRequest{FilePath: src, ClientId: fc.id}
	var reply UnmountResponse
	if err := fc.rpcClient.Call("FileServer.Unmount", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileServer.Unmount error: %v", fc.id, err)
	}
	delete(fc.volumes, target)
	fc.ListAllFiles()
	return nil
}

// user facing method
// creates a file with a relative file name on the server side
func (fc *FileClient) Create(localPath string) (*FileDescriptor, error) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return nil, fmt.Errorf("[file client %s]: %v", fc.id, err)
	}
	v := fc.volumes[mountPoint]
	filepathSuffix := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(v.root, filepathSuffix)
	// create a file descriptor only when the file does not exist
	if fd == nil {
		args := &CreateRequest{FilePath: fp.Join(v.root.Filepath, filepathSuffix), ClientId: fc.id}
		var reply CreateResponse
		if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
			return nil, fmt.Errorf("[file client %s]: call FileServer.Create error: %v", fc.id, err)
		}

		fd := NewFileDescriptor(false, fp.Join(v.root.Filepath, filepathSuffix), 0)
		fd.LastModified = reply.LastModified
		AddToTree(v.root, fd)
		return fd, nil
	} else {
		fc.cache.Set(fd.Filepath, []byte{})
	}
	return fd, nil
}

func (fc *FileClient) checkMountingPoint(file string) (string, error) {
	for mountPoint := range fc.volumes {
		if strings.HasPrefix(file, mountPoint) {
			return mountPoint, nil
		}
	}
	return "", os.ErrNotExist
}

// user facing method
// allows user to open a file path
func (fc *FileClient) Open(localPath string) (*FileDescriptor, error) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return nil, err
	}
	v := fc.volumes[mountPoint]
	filepath := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(v.root, filepath)
	if fd == nil {
		return nil, fmt.Errorf("unable to find file %s", localPath)
	}
	// always read the whole file from the server
	args := &ReadRequest{FilePath: fd.Filepath}
	var reply ReadResponse
	if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
		return nil, fmt.Errorf("call FileServer.Read error: %v", err)
	}
	fc.cache.Set(fd.Filepath, reply.Data)
	if v.fstype == AndrewFileSystemType {
		fd.CallbackPromise = NewCallbackPromise()
	}
	return fd, nil
}

// user facing method
// Idempotent Read Operation:
// stateless read operation, does not change the seeker position of the file descriptor both at the server and client side
// Note: provIded file must be a single file not a directory
func (fc *FileClient) ReadAt(fd *FileDescriptor, offset, n int) ([]byte, error) {
	if fd == nil {
		return nil, fmt.Errorf("invalid read operation, file descriptor is null")
	}
	if fd.IsDir {
		return nil, fmt.Errorf("invalid read operation, current file is a directory")
	}
	// check cache
	if _, err := fc.cache.Get(fd.Filepath); err != nil {
		// not cached
		args := &ReadRequest{FilePath: fd.Filepath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return nil, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		fc.cache.Set(fd.Filepath, reply.Data)
	}
	cached, _ := fc.cache.Get(fd.Filepath)

	if offset > cached.Len() {
		return nil, fmt.Errorf("invalid read: offset exceeds the file length")
	}
	return cached.Bytes()[offset:min(offset+n, cached.Len())], nil
}

// user facing method
// Non-Idempotent Read: read from last seek position recorded at server side
// Note: provided file must be a single file not a directory
func (fc *FileClient) Read(fd *FileDescriptor, n int) ([]byte, error) {
	if fd == nil {
		return nil, fmt.Errorf("invalid read operation, filedescriptor is null")
	}
	if fd.IsDir {
		return nil, fmt.Errorf("invalid read operation, current file is a directory")
	}
	// check cache
	if _, err := fc.cache.Get(fd.Filepath); err != nil {
		// not cached
		args := &ReadRequest{FilePath: fd.Filepath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return nil, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		fc.cache.Set(fd.Filepath, reply.Data)
	}

	cached, _ := fc.cache.Get(fd.Filepath)
	args := &UpdateAttributeRequest{ClientId: fc.id, FilePath: fd.Filepath, FileSeekerIncrement: int64(n)}
	var reply UpdateAttributeResponse
	if err := fc.rpcClient.Call("FileServer.UpdateAttribute", args, &reply); err != nil {
		return nil, fmt.Errorf("call FileServer.UpdateAttribute error: %v", err)
	}
	// update last read end position
	position := int(reply.FileSeekerPosition)

	fmt.Printf("\033[33;1mLast file seeker position at client: %d\n\033[0m", position)
	return cached.Bytes()[position:int(position+n)], nil
}

// user facing method
// Nonidempotent write operation at the given file descriptor location
func (fc *FileClient) Write(fd *FileDescriptor, offset int, data []byte) (int, error) {
	if fd == nil {
		return 0, fmt.Errorf("invalid write operation, filedescriptor is null")
	}
	if fd.IsDir {
		return 0, fmt.Errorf("invalid write operation, current file is a directory")
	}
	// update cached content
	// check cache
	if _, err := fc.cache.Get(fd.Filepath); err != nil {
		// not cached
		args := &ReadRequest{FilePath: fd.Filepath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return 0, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		fc.cache.Set(fd.Filepath, reply.Data)
	}
	cached, _ := fc.cache.Get(fd.Filepath)
	if offset > cached.Len() {
		return 0, fmt.Errorf("invalid write: offset exceeds the file length")
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
	cached.dirty = true
	if v, _, err := fc.find(fd.Filepath); err == nil {
		if v.fstype == SunNetworkFileSystemType {
			// evict cache to server as soon as possible
			args := &WriteRequest{ClientId: fc.id, FilePath: fd.Filepath, Data: cached.Bytes()}
			var reply WriteResponse
			if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
				fc.logger.Printf("ERROR [file client %s] call FileServer.Write error: %v", fc.id, err)
				return 0, err
			} else {
				cached.dirty = false
			}
		}
	}
	return n, nil
}

// user facing method
// close the file descriptor
func (fc *FileClient) Close(fd *FileDescriptor) {
	if fd == nil {
		return
	}
	if fd.IsDir {
		return // will never be cached so no update to server
	}
	cached, err := fc.cache.Get(fd.Filepath)
	if err != nil {
		return // not cached
	}
	if !cached.dirty {
		return
	}
	// find the volume mounting type
	// if the mounting type is NFS, we donot need to update the server
	if v, _, err := fc.find(fd.Filepath); err == nil {
		if v.fstype == SunNetworkFileSystemType {
			return
		}
	}
	// evict cache to server
	args := &WriteRequest{ClientId: fc.id, FilePath: fd.Filepath, Data: cached.Bytes()}
	var reply WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		fc.logger.Printf("ERROR [file client %s] call FileServer.Write error: %v", fc.id, err)
		return
	}
	cached.dirty = false
}

// user facing method
// to display all the mounted files
func (fc *FileClient) ListAllFiles() {
	fmt.Printf("[file client %s] local file tree:\n", fc.id)
	for root, v := range fc.volumes {
		PrintTree(root, v.root)
	}
}

// user facing method
// to display all files under a given directory, essentially a `ls` command
func (fc *FileClient) ListFiles(path string) {
	mountPoint, err := fc.checkMountingPoint(path)
	if err != nil {
		fc.logger.Printf("ERROR [file client %s]: %v", fc.id, err)
		return
	}
	fmt.Printf("[file client %s] local file tree:\n", fc.id)
	v := fc.volumes[mountPoint]
	PrintTree(path, v.root)
}

// server facing method i.e. rpc
// an endpoint to allow server to update the callback promise
func (fc *FileClient) UpdateCallbackPromise(req UpdateCallbackPromiseRequest, resp *UpdateCallbackPromiseResponse) error {
	fc.logger.Printf("INFO [file client %s] FileClient.UpdateCallbackPromise is called", fc.id)
	_, fd, _ := fc.find(req.FilePath)
	if fd == nil || fd.CallbackPromise == nil {
		return nil
	}
	fd.CallbackPromise.Set(req.IsValidOrCanceled)
	fc.logger.Printf("INFO [file client %s]: content in %s has updated\n", fc.id, req.FilePath)
	return nil
}

func (fc *FileClient) find(filepath string) (*Volume, *FileDescriptor, error) {
	for _, v := range fc.volumes {
		found := Search(v.root, filepath)
		if found != nil {
			return v, found, nil
		}
	}
	return nil, nil, os.ErrNotExist
}

func (fc *FileClient) Shutdown() {
	fc.stop <- struct{}{}
	fc.rpcServer.Shutdown()
}
