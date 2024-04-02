package service

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"distributed-file-system/pkg/golang/logger"
	"distributed-file-system/pkg/golang/rpc"
)

type FileServer struct {
	addr              string
	rpcServer         *rpc.Server
	exportedRootPaths []string                   // top level directory path that the server is exporting
	fileIndexTrees    map[string]*FileDescriptor // key: exported root path, value: fd, each fd must be independent of other
	logger            *logger.Logger
}

// return the root, file path since the root, and error
func (fs *FileServer) find(file string) (string, string, error) {
	// check if the file has been exported
	var root, path string
	var err error
	for _, exportedRootPath := range fs.exportedRootPaths {
		err = filepath.Walk(exportedRootPath, func(currentPath string, info os.FileInfo, err error) error {
			if strings.HasSuffix(currentPath, file) {
				root = exportedRootPath
				path = strings.TrimPrefix(currentPath, exportedRootPath)
				return nil
			}
			return nil
		})
	}
	if err != nil {
		return "", "", err
	} else if root == "" {
		return "", "", os.ErrNotExist
	}
	return root, path, nil
}

func (fs *FileServer) Mount(req MountRequest, resp *MountResponse) error {
	// every filepath is found through root + path for security
	rootpath, path, err := fs.find(req.FilePath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	rootfd := fs.fileIndexTrees[rootpath]
	fd := Search(rootfd, path)
	if fd == nil {
		return fmt.Errorf("file server: no such file") // should never happen
	}
	if FileSystemType(req.FileSystemType) == AndrewFileSystemType {
		// the client operates in an andrew filesystem way
		// server records the client and sent back a callback promise
		// the callback promise is initialized as valid
		Subscribe(fd, req.ClientId, req.ClientAddr)
		resp.CallbackPromise = true
	}
	resp.IsDir = fd.IsDir
	resp.FilePath = fd.Filepath
	resp.Size = int64(fd.Size)
	childrenPaths := ""
	for _, cfd := range fd.Children {
		childrenPaths = fmt.Sprintf("%s:%s", childrenPaths, cfd.Filepath)
	}
	resp.ChildrenPaths = childrenPaths
	resp.LastModified = fd.LastModified
	return nil
}

// unmount will unsubscribe the requested client from the list
func (fs *FileServer) Unmount(req UnmountRequest, resp *UnmountResponse) error {
	root, path, err := fs.find(req.FilePath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	rootfd := fs.fileIndexTrees[root]
	fd := Search(rootfd, path)
	Unsubscribe(fd, req.ClientId)
	resp.IsSuccess = true
	return nil
}

func (fs *FileServer) GetAttribute(req GetAttributeRequest, resp *GetAttributeResponse) error {
	root, path, err := fs.find(req.FilePath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	rootfd := fs.fileIndexTrees[root]
	fd := Search(rootfd, path)
	resp.IsDir = fd.IsDir
	resp.FilePath = fd.Filepath
	resp.FileSeeker = int64(fd.Seeker)
	resp.LastModified = fd.LastModified
	return nil
}

// updates the seeker position of the file descriptor
func (fs *FileServer) UpdateAttribute(req UpdateAttributeRequest, resp *UpdateAttributeResponse) error {
	fs.logger.Printf("INFO [file server] FileServer.UpdateAttribute is called")
	root, path, err := fs.find(req.FilePath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	rootfd := fs.fileIndexTrees[root]
	fd := Search(rootfd, path)
	incr := uint64(req.FileSeekerIncrement)
	if fd.Seeker+incr > fd.Size {
		return fmt.Errorf("file server: invalid read, offset exceeds the file length")
	}
	resp.FileSeekerPosition = int64(fd.Seeker)

	fmt.Printf("\033[33;1mLast file seeker position at server: %d\n\033[0m", resp.FileSeekerPosition)
	// update the seeker position at server side
	fd.Seeker = fd.Seeker + incr
	resp.IsSuccess = true
	return nil
}

// idempotent operation
func (fs *FileServer) Create(req CreateRequest, resp *CreateResponse) error {
	fs.logger.Printf("INFO [file server] FileServer.Create is called")
	parentDir := filepath.Dir(req.FilePath)
	root, _, err := fs.find(parentDir)
	if err != nil {
		return err
	}
	// check if the parent directory exists
	if _, err := os.Stat(filepath.Join(root, parentDir)); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("file server: dir %s does not exist", parentDir)
	}
	rootfd := fs.fileIndexTrees[root]
	pfd := Search(rootfd, parentDir)
	if pfd == nil {
		return fmt.Errorf("file server: parent dir %s does not exist", parentDir)
	}
	// check if the file exists
	localPath := filepath.Join(root, req.FilePath)
	if _, err := os.Stat(localPath); errors.Is(err, os.ErrNotExist) {
		_, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("file server: create error %v", err)
		}
		fd := NewFileDescriptor(false, req.FilePath, 0)
		fd.subscription = NewSubscription(fs.logger)
		fd.LastModified = time.Now().Unix()
		// add to tree
		pfd.AddChild(fd)
		// update the registered client
		args := &UpdateCallbackPromiseRequest{
			FilePath:          pfd.Filepath,
			IsValidOrCanceled: false,
		}
		pfd.subscription.Broadcast(req.ClientId, args) // tell everyone who listens on the parent directory that there is a change to the parent directory
		resp.IsSuccess = true
	} else {
		// overwrite the file if it already exists
		_, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("file server: create error %v", err)
		}
	}

	return nil
}

// Read operation sends the entire file content to the client
func (fs *FileServer) Read(req ReadRequest, resp *ReadResponse) error {
	fs.logger.Printf("INFO [file server] FileServer.Read is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		return fmt.Errorf("file server: open error: %v", err)
	}
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("file server: open error: %v", err)
	}
	resp.Data = append(resp.Data, data...)
	return nil
}

// Write operation writes the byte data to the specified file
func (fs *FileServer) Write(req WriteRequest, resp *WriteResponse) error {
	fs.logger.Printf("INFO [file server] FileServer.Write is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
	// overwrites the original data
	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	defer f.Close()
	_, err = f.Write(req.Data)
	if err != nil {
		return fmt.Errorf("file server: write error %v", err)
	}
	fd := Search(fs.fileIndexTrees[root], req.FilePath)
	fd.LastModified = time.Now().Unix()
	// if the client choose not to register here, no update would be seen at the client side
	args := &UpdateCallbackPromiseRequest{
		FilePath:          req.FilePath,
		IsValidOrCanceled: false,
	}
	fd.subscription.Broadcast(req.ClientId, args)
	return nil
}

func NewFileServer(addr string) *FileServer {
	exportedRootPaths := os.Getenv("EXPORT_ROOT_PATHS")
	paths := strings.Split(exportedRootPaths, ":")
	logger := logger.NewLogger("./server.log")
	fs := &FileServer{
		addr:              addr,
		exportedRootPaths: paths,
		fileIndexTrees:    make(map[string]*FileDescriptor),
		logger:            logger,
		rpcServer:         rpc.NewServer(logger),
	}
	for _, path := range paths {
		fs.fileIndexTrees[path] = fs.buildFileIndexTree(path)
	}
	if err := fs.rpcServer.Register(fs); err != nil {
		panic(fmt.Sprintf("rpc register error: %v", err))
	}
	return fs
}

func (fs *FileServer) buildFileIndexTree(entry string) *FileDescriptor {
	if entry == "" {
		panic("no exported directories")
	}
	info, _ := os.Stat(entry)
	root := NewFileDescriptor(info.IsDir(), "", uint64(info.Size()))
	root.subscription = NewSubscription(fs.logger)
	parents := make(map[string]*FileDescriptor)
	parents[entry] = root
	err := filepath.Walk(entry, func(currentPath string, info os.FileInfo, err error) error {
		if currentPath == entry {
			return nil
		}
		pfd := parents[filepath.Dir(currentPath)]
		cfd := NewFileDescriptor(info.IsDir(), strings.TrimPrefix(currentPath, entry), uint64(info.Size()))
		cfd.subscription = NewSubscription(fs.logger)
		pfd.AddChild(cfd)
		if _, ok := parents[currentPath]; !ok {
			parents[currentPath] = cfd
		}
		return nil
	})

	if err != nil {
		panic(fmt.Sprintf("file server build tree error: %v", err))
	}
	return root
}

func (fs *FileServer) Run() {
	s, err := net.ResolveUDPAddr("udp", fs.addr)
	if err != nil {
		panic(fmt.Sprintf("server error resolving udp address: %v", err))
	}
	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		panic(fmt.Sprintf("network error: %v", err))
	}
	fs.logger.Printf("INFO [file server]: listening on %s", conn.LocalAddr().String())
	fs.rpcServer.Accept(conn)
}
