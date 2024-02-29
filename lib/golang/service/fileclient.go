package service

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	fp "path/filepath"
	"strings"
	"time"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/rpc"
)

const (
	duration     int           = 20
	pollInterval time.Duration = 1 * time.Second
)

type FileClient struct {
	id        string
	addr      string
	rpcClient *rpc.Client
	rpcServer *rpc.Server
	stop      chan struct{}
	volumnes  map[string]*Volume // mounted files
}

func NewFileClient(id, addr string, config *config.Config) *FileClient {
	fc := &FileClient{
		id:       id,
		addr:     addr,
		stop:     make(chan struct{}),
		volumnes: make(map[string]*Volume),
	}
	fc.rpcServer = rpc.NewServer()
	if err := fc.rpcServer.Register(fc); err != nil {
		log.Fatalf("file client rpc register error: %v", err)
		return nil
	}
	rpcClient, err := rpc.Dial(config.RemoteAddr)
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
	root := reply.Fd
	fc.volumnes[target] = NewVolume(root, fstype)
	fc.ListFiles(target)
	// NFS requires polling at the client side
	if fstype == SunNetworkFileSystemType {
		go fc.poll()
	}
	// duration <= 0 means mount forever, the files will be mounted until client explicitly call unmount
	if duration > 0 {
		go fc.monitor(src, target)
	}
	return nil
}

func (fc *FileClient) monitor(src, target string) error {
	<-time.After(time.Duration(duration) * time.Second)
	fc.stop <- struct{}{}
	log.Printf("[file client %s]: timeout, unmounting file: %s", fc.id, target)
	return fc.Unmount(src, target)
}

func (fc *FileClient) poll() {
	for {
		select {
		case <-fc.stop:
			return
		default:
			// check for all cached(open) file
			for _, v := range fc.volumnes {
				for filepath, entry := range v.cache.cc {
					now := time.Now()
					if now.Sub(entry.lastValidated) < pollInterval {
						continue // consider valid
					}
					getArgs := &GetAttributeRequest{ClientId: fc.id, FilePath: filepath}
					var getReply GetAttributeResponse
					if err := fc.rpcClient.Call("FileServer.GetAttribute", getArgs, &getReply); err != nil {
						log.Printf("[file client %s]: call FileServer.GetAttribute error: %v", fc.id, err)
						continue
					}
					fd := Search(v.root, filepath)
					lastModifiedAtServer := getReply.Fd.lastModified
					if lastModifiedAtServer == fd.lastModified {
						// no change at the server, update the lastValidated timestamp
						entry.lastValidated = now
						continue
					}
					// invalidated the entry
					readArgs := &ReadRequest{ClientId: fc.id, FilePath: fd.FilePath}
					var readReply ReadResponse
					if err := fc.rpcClient.Call("FileServer.Read", readArgs, &readReply); err != nil {
						log.Printf("call FileServer.Read error: %v", err)
						continue
					}
					entry.Reset()
					entry.Write(readReply.Data)
					entry.lastValidated = now
				}
			}
		}
	}
}

func (fc *FileClient) Unmount(src, target string) error {
	args := &UnmountRequest{FilePath: src, ClientId: fc.id}
	var reply UnmountResponse
	if err := fc.rpcClient.Call("FileServer.Unmount", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileServer.Unmount error: %v", fc.id, err)
	}
	delete(fc.volumnes, target)
	fc.ListAllFiles()
	return nil
}

// create a file
func (fc *FileClient) Create(localPath string) error {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return fmt.Errorf("[file client %s]: %v", fc.id, err)
	}
	v := fc.volumnes[mountPoint]
	filepath := strings.TrimPrefix(localPath, mountPoint)
	args := &CreateRequest{FilePath: fp.Join(v.root.FilePath, filepath)}
	var reply CreateResponse
	if err := fc.rpcClient.Call("FileServer.Create", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileServer.Create error: %v", fc.id, err)
	}
	AddToTree(v.root, reply.Fd)
	return nil
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

func (fc *FileClient) checkMountingPoint(file string) (string, error) {
	for mountPoint := range fc.volumnes {
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
	v := fc.volumnes[mountPoint]
	filepath := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(v.root, filepath)
	if fd.IsDir {
		return nil, fmt.Errorf("invalId read operation, %s is a directory", localPath)
	}

	// check cache
	if _, err := v.cache.Get(fd.FilePath); err != nil {
		// not cached
		args := &ReadRequest{FilePath: fd.FilePath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return nil, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		v.cache.Set(fd.FilePath, reply.Data)
	}

	cached, _ := v.cache.Get(fd.FilePath)
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
	v := fc.volumnes[mountPoint]
	filepath := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(v.root, filepath)
	if fd.IsDir {
		return nil, fmt.Errorf("invalId read operation, %s is a directory", localPath)
	}
	// check cache
	if _, err := v.cache.Get(fd.FilePath); err != nil {
		// not cached
		args := &ReadRequest{FilePath: fd.FilePath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return nil, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		v.cache.Set(fd.FilePath, reply.Data)
	}

	cached, _ := v.cache.Get(fd.FilePath)
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
	v := fc.volumnes[mountPoint]
	filepath := strings.TrimPrefix(localPath, mountPoint)
	fd := Search(v.root, filepath)
	if fd.IsDir {
		return 0, fmt.Errorf("invalId write operation, %s is a directory", localPath)
	}

	// update cached content
	// check cache
	if _, err := v.cache.Get(fd.FilePath); err != nil {
		// not cached
		args := &ReadRequest{FilePath: fd.FilePath}
		var reply ReadResponse
		if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
			return 0, fmt.Errorf("call FileServer.Read error: %v", err)
		}
		v.cache.Set(fd.FilePath, reply.Data)
	}
	cached, _ := v.cache.Get(fd.FilePath)
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
	args := &WriteRequest{ClientId: fc.id, FilePath: fd.FilePath, Data: cached.Bytes()}
	var reply WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		return 0, fmt.Errorf("call FileServer.Write error: %v", err)
	}
	return n, nil
}

func (fc *FileClient) Remove(localPath string) error {
	mountPoint, err := fc.checkMountingPoint(localPath)
	if err != nil {
		return fmt.Errorf("[file client %s]: %v", fc.id, err)
	}
	v := fc.volumnes[mountPoint]
	filepath := strings.TrimPrefix(localPath, mountPoint)
	args := &RemoveRequest{ClientId: fc.id, FilePath: fp.Join(v.root.FilePath, filepath)}
	var reply RemoveResponse
	if err := fc.rpcClient.Call("FileServer.Remove", args, &reply); err != nil {
		return fmt.Errorf("[file client %s]: call FileSever.Remove error: %v", fc.id, err)
	}
	log.Printf("reply: remove status %v", reply.IsRemoved)
	RemoveFromTree(v.root, fp.Join(v.root.FilePath, filepath))
	return nil
}

func (fc *FileClient) Shutdown() {
	fc.stop <- struct{}{}
	fc.rpcServer.Shutdown()
}

// utility function for display purpose
func (fc *FileClient) ListAllFiles() {
	fmt.Printf("local file tree:\n")
	for root, v := range fc.volumnes {
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
	v := fc.volumnes[mountPoint]
	PrintTree(path, v.root)
}
