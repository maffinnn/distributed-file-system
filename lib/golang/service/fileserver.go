package service

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"distributed-file-system/lib/golang/rpc"
)

type FileServer struct {
	addr              string
	rpcServer         *rpc.Server
	exportedRootPaths []string                   // top level directory path that the server is exporting
	fileIndexTrees    map[string]*FileDescriptor // key: exported root path, value: fd, each fd must be independent of other
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
	if req.ClientId != "" && req.ClientAddr != "" {
		// the client operates in an andrew filesystem way
		// server records the client and sent back a callback promise
		// the callback promise is initialized as valid
		Subscribe(fd, req.ClientId, req.ClientAddr)
	}
	resp.IsDir = fd.IsDir
	resp.FilePath = fd.Filepath
	childrenPaths := ""
	for _, cfd := range fd.Children {
		childrenPaths = fmt.Sprintf("%s:%s", childrenPaths, cfd.Filepath)
	}
	resp.ChildrenPaths = childrenPaths
	resp.LastModified = fd.LastModified
	resp.CallbackPromise = true
	return nil
}

// unmount will unsubscribe the requested client from the list
func (fs *FileServer) Unmount(req UnmountRequest, resp *UnmountResponse) error {
	log.Printf("FileServer.Unmount is called")
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
	resp.LastModified = fd.LastModified
	return nil
}

func (fs *FileServer) Create(req CreateRequest, resp *CreateResponse) error {
	log.Printf("FileServer.Create is called")
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
		return fmt.Errorf("file server: dir %s does not exist", parentDir)
	}
	// check if the file exists
	localPath := filepath.Join(root, req.FilePath)
	if _, err := os.Stat(localPath); errors.Is(err, os.ErrNotExist) {
		_, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("file server: create error %v", err)
		}
		fd := NewFileDescriptor(false, req.FilePath)
		fd.subscription = NewSubscription()
		fd.LastModified = time.Now().Unix()
		fd.owner = req.ClientId
		// add to tree
		pfd.AddChild(fd)
		resp.IsSuccess = true
		return nil
	}
	return fmt.Errorf("file server: %s already exists", filepath.Base(req.FilePath))
}

// func (fs *FileServer) MkDir(req CreateRequest, resp *CreateResponse) error {
// 	log.Printf("FileServer.MkDir is called")
// 	req.FilePath = strings.TrimPrefix(req.FilePath, string(os.PathSeparator))
// 	parts := strings.Split(req.FilePath, string(os.PathSeparator))
// 	root, _, err := fs.find(parts[0])
// 	if err != nil {
// 		return err
// 	}
// 	pfd := fs.fileIndexTrees[root]
// 	since := -1
// 	fds := make([]*FileDescriptor, 0)
// 	for i, _ := range parts {
// 		path := filepath.Join(parts[:i+1]...)
// 		localDir := filepath.Join(root, path)
// 		// check if the directory exists
// 		if _, err := os.Stat(localDir); os.IsNotExist(err) {
// 			if since == -1 {
// 				since = i
// 			}
// 			if err := os.Mkdir(localDir, os.ModePerm); err != nil {
// 				return fmt.Errorf("file server: error creating directory %s, err: %v", path, err)
// 			}
// 			fd := NewFileDescriptor(true, path)
// 			fd.Owner = req.ClientId
// 			fds = append(fds, fd)
// 			pfd.AddChild(fd)
// 			pfd = fd
// 		} else {
// 			//exists
// 			pfd = pfd.FindChildWithFilePath(path)
// 			fds = append(fds, pfd)
// 		}
// 	}
// 	if since == -1 {
// 		return fmt.Errorf("file server: dir: %s exists", req.FilePath)
// 	} else {
// 		var i int
// 		if since > 0 {
// 			i = since - 1 // because the parent directory also needs to update last modified time
// 		} else {
// 			i = since
// 		}
// 		timestamp := time.Now().Unix()
// 		for ; i < len(fds); i++ {
// 			fds[i].lastModified = timestamp
// 		}
// 	}
// 	resp.Fd = fds[since]
// 	return nil
// }

// func (fs *FileServer) RmDir(req RemoveRequest, resp *RemoveResponse) error {
// 	log.Printf("FileServer.RmDir is called")
// 	root, _, err := fs.find(req.FilePath)
// 	if err != nil {
// 		return err
// 	}
// 	localPath := filepath.Join(root, req.FilePath)
// 	if err := os.RemoveAll(localPath); err != nil {
// 		resp.IsRemoved = false
// 		return fmt.Errorf("file server: remove %s error: %v", req.FilePath, err)
// 	}
// 	// update on tree
// 	rootfd := fs.fileIndexTrees[root]
// 	pfd := Search(rootfd, filepath.Dir(req.FilePath))
// 	// TODO:
// 	// pfd.Sub.Publish(DirectoryUpdateTopic, )
// 	pfd.RemoveChild(req.FilePath)
// 	resp.IsRemoved = true
// 	return nil
// }

func (fs *FileServer) Read(req ReadRequest, resp *ReadResponse) error {
	// log.Printf("FileServer.Read is called")
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

func (fs *FileServer) Write(req WriteRequest, resp *WriteResponse) error {
	log.Printf("FileServer.Write is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
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
	fd.subscription.Broadcast(req.ClientId, FileUpdateTopic, args)
	return nil
}

func (fs *FileServer) Remove(req RemoveRequest, resp *RemoveResponse) error {
	log.Printf("FileServer.Remove is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
	if err = os.Remove(localPath); err != nil {
		resp.IsRemoved = false
		return fmt.Errorf("file server: %v", err)
	}
	// update on tree
	rootfd := fs.fileIndexTrees[root]
	pfd := Search(rootfd, filepath.Dir(req.FilePath))
	pfd.RemoveChild(req.FilePath)
	pfd.LastModified = time.Now().Unix()
	resp.IsRemoved = true
	args := &UpdateCallbackPromiseRequest{
		FilePath:          req.FilePath,
		IsValidOrCanceled: false,
	}
	pfd.subscription.Broadcast(req.ClientId, FileUpdateTopic, args)
	return nil
}

func NewFileServer(addr string) *FileServer {
	exportedRootPaths := os.Getenv("EXPORT_ROOT_PATHS")
	paths := strings.Split(exportedRootPaths, ":")
	fs := &FileServer{
		addr:              addr,
		exportedRootPaths: paths,
		fileIndexTrees:    make(map[string]*FileDescriptor),
	}
	for _, path := range paths {
		fs.fileIndexTrees[path] = buildTree(path)
	}
	fs.rpcServer = rpc.NewServer()
	if err := fs.rpcServer.Register(fs); err != nil {
		log.Fatal("rpc register error:", err)
	}
	return fs
}

func buildTree(entry string) *FileDescriptor {
	if entry == "" {
		panic("no exported directories")
	}
	info, _ := os.Stat(entry)
	root := NewFileDescriptor(info.IsDir(), "")
	root.subscription = NewSubscription()
	parents := make(map[string]*FileDescriptor)
	parents[entry] = root
	err := filepath.Walk(entry, func(currentPath string, info os.FileInfo, err error) error {
		if currentPath == entry {
			return nil
		}
		pfd := parents[filepath.Dir(currentPath)]
		cfd := NewFileDescriptor(info.IsDir(), strings.TrimPrefix(currentPath, entry))
		cfd.subscription = NewSubscription()
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
		log.Fatal("client error resolving udp address: ", err)
	}
	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Printf("file server is listening on %s", conn.LocalAddr().String())
	fs.rpcServer.Accept(conn)
}
