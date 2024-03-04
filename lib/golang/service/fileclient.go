package service

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	fp "path/filepath"
	"strings"
	"sync"
	"time"

	"distributed-file-system/lib/golang/rpc"
)

var (
	Duration     int = 30 // in seconds
	PollInterval int = 10 // in miliseconds
)

type FileClient struct {
	id        string
	addr      string
	rpcClient *rpc.Client
	rpcServer *rpc.Server
	stop      chan struct{}
	volumes   map[string]*Volume // mounted files
	cache     *Cache
}

func NewFileClient(id, addr, serverAddr string) *FileClient {
	fc := &FileClient{
		id:      id,
		addr:    addr,
		stop:    make(chan struct{}),
		volumes: make(map[string]*Volume),
		cache:   NewCache(),
	}
	fc.rpcServer = rpc.NewServer()
	if err := fc.rpcServer.Register(fc); err != nil {
		log.Fatalf("file client rpc register error: %v", err)
		return nil
	}
	rpcClient, err := rpc.Dial(serverAddr)
	if err != nil {
		log.Fatalf("file client rpc dial error: %v", err)
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
	log.Printf("[file client %s]: listening on %s", fc.id, conn.LocalAddr().String())
	fc.rpcServer.Accept(conn)
}

func (fc *FileClient) Mount(src, target string, fstype FileSystemType) error {
	args := &MountRequest{FilePath: src}
	if fstype == AndrewFileSystemType {
		args.ClientId = fc.id
		args.ClientAddr = fc.addr
	}
	var reply MountResponse
	if err := fc.rpcClient.Call("FileServer.Mount", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileServer.Mount error: %v", fc.id, err)
	}
	log.Printf("[file client %s]: %s is mounted at %v", fc.id, src, target)
	root := NewFileDescriptor(reply.IsDir, reply.FilePath)
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
	fd := NewFileDescriptor(reply.IsDir, reply.FilePath)
	root.Children = append(root.Children, fd)
	childrenPaths := strings.Split(reply.ChildrenPaths, ":")
	for _, cp := range childrenPaths {
		fc.mountRecursive(fd, cp, fstype)
	}
}

func (fc *FileClient) monitor(src, target string) error {
	<-time.After(time.Duration(Duration) * time.Second)
	fc.stop <- struct{}{}
	log.Printf("[file client %s]: timeout, unmounting file: %s", fc.id, target)
	return fc.Unmount(src, target)
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
						log.Printf("[file client %s]: call FileServer.GetAttribute error: %v", fc.id, err)
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
					readArgs := &ReadRequest{ClientId: fc.id, FilePath: fd.Filepath}
					var readReply ReadResponse
					if err := fc.rpcClient.Call("FileServer.Read", readArgs, &readReply); err != nil {
						log.Printf("call FileServer.Read error: %v", err)
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

func (fc *FileClient) Unmount(src, target string) error {
	args := &UnmountRequest{FilePath: src, ClientId: fc.id}
	var reply UnmountResponse
	if err := fc.rpcClient.Call("FileServer.Unmount", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileServer.Unmount error: %v", fc.id, err)
	}
	delete(fc.volumes, target)
	fc.ListAllFiles()
	return nil
}

// create a file
func (fc *FileClient) Create(localPath string) (*FileDescriptor, error) {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return nil, fmt.Errorf("[file client %s]: %v", fc.id, err)
	}
	v := fc.volumes[mountPoint]
	filepathSuffix := strings.TrimPrefix(localPath, mountPoint)
	args := &CreateRequest{FilePath: fp.Join(v.root.Filepath, filepathSuffix)}
	var reply CreateResponse
	if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
		return nil, fmt.Errorf("[file client %s]: call FileServer.Create error: %v", fc.id, err)
	}

	fd := NewFileDescriptor(false, fp.Join(v.root.Filepath, filepathSuffix))
	fd.LastModified = reply.LastModified
	AddToTree(v.root, fd)
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
	if v.fstype == AndrewFileSystemType {
		fd.CallbackPromise = NewCallbackPromise()
	}
	return fd, nil
}

// Idempotent Read Operation:
// stateless read operation, does not change the seeker position of the file descriptor
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
	if offset >= cached.Len() {
		return nil, fmt.Errorf("invalid read: offset exceeds the file length")
	}
	return cached.Bytes()[offset:min(offset+n, cached.Len())], nil
}

// Non-Idempotent Read: read from last seek position recorded at client sIde fd
// Note: provIded file must be a single file not a directory
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
	// update last read end position
	lastOffset := int(fd.Seeker)
	if lastOffset >= cached.Len() {
		return nil, fmt.Errorf("EOF has reached")
	}
	fd.Seeker = uint64(min(lastOffset+n, cached.Len()))
	return cached.Bytes()[lastOffset:int(fd.Seeker)], nil
}

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
	if offset >= cached.Len() {
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
				log.Printf("call FileServer.Write error: %v", err)
				return 0, err
			} else {
				cached.dirty = false
			}
		}
	}
	return n, nil
}

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
	// evict cache to server
	args := &WriteRequest{ClientId: fc.id, FilePath: fd.Filepath, Data: cached.Bytes()}
	var reply WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		log.Printf("call FileServer.Write error: %v", err)
		return
	}
	cached.dirty = false
}

func (fc *FileClient) UpdateCallbackPromise(req UpdateCallbackPromiseRequest, resp *UpdateCallbackPromiseResponse) error {
	log.Printf("FileClient.UpdateCallbackPromise is called")
	_, fd, _ := fc.find(req.FilePath)
	if fd == nil || fd.CallbackPromise == nil {
		return nil
	}
	fd.CallbackPromise.Set(req.IsValidOrCanceled)
	fmt.Printf("INFO [file client %s]: content in %s has updated\n", fc.id, req.FilePath)
	return nil
}

func (fc *FileClient) Remove(fd *FileDescriptor) error {
	if fd == nil {
		return fmt.Errorf("invalid remove operation, filedescriptor is null")
	}
	args := &RemoveRequest{ClientId: fc.id, FilePath: fd.Filepath}
	var reply RemoveResponse
	if err := fc.rpcClient.Call("FileServer.Remove", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileSever.Remove error: %v", fc.id, err)
	}
	// search all the volumnes
	for _, v := range fc.volumes {
		found := Search(v.root, fd.Filepath)
		if found != nil {
			log.Printf("reply: remove status %v", reply.IsRemoved)
			RemoveFromTree(v.root, fd.Filepath)
			return nil
		}
	}
	return fmt.Errorf("[file client %s] error removing the file", fc.id)
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

// utility function for display purpose
func (fc *FileClient) ListAllFiles() {
	fmt.Printf("local file tree:\n")
	for root, v := range fc.volumes {
		PrintTree(root, v.root)
	}
}

func (fc *FileClient) ListFiles(path string) {
	mountPoint, err := fc.checkMountingPoint(path)
	if err != nil {
		log.Printf("[file client %s]: %v", fc.id, err)
		return
	}
	fmt.Printf("local file tree:\n")
	v := fc.volumes[mountPoint]
	PrintTree(path, v.root)
}

// func (fc *FileClient) MakeDir(dir string) error {
// 	mountPoint, err := fc.checkMountingPoint(dir)
// 	if err != nil {
// 		return fmt.Errorf("[file client %s]: %v", fc.id, err)
// 	}
// 	fd := fc.mountedFiles[mountPoint]
// 	suffix := strings.TrimPrefix(dir, mountPoint)
// 	args := &CreateRequest{FilePath: filepath.Join(fd.FilePath, suffix)}
// 	var reply CreateResponse
// 	if err := fc.rpcClient.Call("FileServer.MkDir", args, &reply); err != nil {
// 		return fmt.Errorf("[file client %s]: call FileServer.MkDir error: %v", fc.id, err)
// 	}
// 	AddToTree(fd, reply.Fd)
// 	return nil
// }

// func (fc *FileClient) RemoveDir(dir string) error {
// 	mountPoint, err := fc.checkMountingPoint(dir)
// 	if err != nil {
// 		return fmt.Errorf("[file client %s]: %v", fc.id, err)
// 	}
// 	fd := fc.mountedFiles[mountPoint]
// 	suffix := strings.TrimPrefix(dir, mountPoint)
// 	args := &RemoveRequest{FilePath: filepath.Join(fd.FilePath, suffix)}
// 	var reply RemoveResponse
// 	if err := fc.rpcClient.Call("FileServer.RmDir", args, &reply); err != nil {
// 		return fmt.Errorf("[file client %s]: call FileServer.RmDir error: %v", fc.id, err)
// 	}
// 	// remove the fd from the tree
// 	RemoveFromTree(fd, filepath.Join(fd.FilePath, suffix))
// 	return nil
// }
