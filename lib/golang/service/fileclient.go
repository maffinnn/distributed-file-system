package service

import (
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
	id           string
	addr		 string
	rpcClient    *rpc.Client
	rpcServer 	 *rpc.Server
	mountedFiles map[string]*FileDescriptor // translate local file path to server side file path
}

func NewFileClient(id, addr string, config *config.Config) *FileClient {
	fc := &FileClient{
		id:           id,
		addr:		 addr,
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
	log.Printf("file client is listening on %s", conn.LocalAddr().String())
	fc.rpcServer.Accept(conn)
}

// TODO: check permission when client is mounting
func (fc *FileClient) Mount(src, target string, timeout int) {
	args := &MountRequest{FilePath: src, ClientId: fc.id, ClientAddr: fc.addr}
	var reply MountResponse
	if err := fc.rpcClient.Call("FileServer.Mount", args, &reply); err != nil {
		log.Printf("call FileServer.Mount error: %v", err)
		return
	}
	log.Printf("file client: %s is mounted at %v", src, target)
	fc.mountedFiles[target] = reply.Fd
	fc.PrintFiles(target)
	// timeout <= 0 means no timeout, the files will be mounted until client explicitly call unmount
	if timeout > 0 {
		go fc.monitor(src, target, timeout)
	}
}

func (fc *FileClient) monitor(src, target string, timeout int) {
	<-time.After(time.Duration(timeout) * time.Second)
	fc.Unmount(src, target)
	log.Printf("file server: timeout, unmounting files: %s", target)
}

func (fc *FileClient) Unmount(src, target string) {
	args := &UnmountRequest{FilePath: src, ClientId: fc.id}
	var reply UnmountResponse
	if err := fc.rpcClient.Call("FileServer.Unmount", args, &reply); err != nil {
		log.Printf("call FileServer.Unmount error: %v", err)
		return
	}
	delete(fc.mountedFiles, target)
	fc.PrintAllFiles()
}

func (fc *FileClient) Create(localfilepath string) {
	mountPoint, err := fc.checkMountingPoint(localfilepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localfilepath, mountPoint)
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
		log.Printf("file client: %v", err)
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

func (fc *FileClient) Read(localfilepath string, offset, n int64) {
	mountPoint, err := fc.checkMountingPoint(localfilepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localfilepath, mountPoint)
	args := &ReadRequest{FilePath: filepath.Join(fd.FilePath, suffix), Offset: offset, N: n}
	var reply ReadResponse
	if err := fc.rpcClient.Call("FileServer.Read", args, &reply); err != nil {
		log.Printf("call FileServer.Read error: %v", err)
		return
	}
	log.Printf("reply: %s", string(reply.Content))
}

func (fc *FileClient) Write(localfilepath string, offset int64, data []byte) {
	mountPoint, err := fc.checkMountingPoint(localfilepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localfilepath, mountPoint)
	args := &WriteRequest{FilePath: filepath.Join(fd.FilePath, suffix), Offset: offset, Data: data}
	var reply WriteResponse
	if err := fc.rpcClient.Call("FileServer.Write", args, &reply); err != nil {
		log.Printf("call FileServer.Write error: %v", err)
		return
	}
	log.Printf("reply: %d bytes write to the file", reply.N)
}

func (fc *FileClient) Remove(localfilepath string) {
	mountPoint, err := fc.checkMountingPoint(localfilepath)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	suffix := strings.TrimPrefix(localfilepath, mountPoint)
	args := &RemoveRequest{FilePath: filepath.Join(fd.FilePath, suffix)}
	var reply RemoveResponse
	if err := fc.rpcClient.Call("FileServer.Remove", args, &reply); err != nil {
		log.Printf("call FileSever.Remove error: %v", err)
		return
	}
	log.Printf("reply: remove status %v", reply.IsRemoved)
	RemoveFromTree(fd, filepath.Join(fd.FilePath, suffix))
}

// client rpc
func (fc *FileClient) Update(req UpdateRequest, resp *UpdateResponse) error {
	log.Printf("FileClient.Update is called")
	return nil
}

func (fc *FileClient) Shutdown() {
	fc.rpcServer.Shutdown()
}



//
// utility function for display purpose
func (fc *FileClient) PrintAllFiles() {
	for entry, fd := range fc.mountedFiles {
		PrintTree(entry, fd)
	}
}

func (fc *FileClient) PrintFiles(entry string) {
	mountPoint, err := fc.checkMountingPoint(entry)
	if err != nil {
		log.Printf("file client: %v", err)
		return
	}
	fd := fc.mountedFiles[mountPoint]
	PrintTree(entry, fd)
}
