package service

import (
	"bytes"
	"distributed-file-system/lib/golang/service/file"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/proto"
)

type FileServer struct {
	addr              string
	exportedRootPaths []string                        // top level directory path that the server is exporting
	trees             map[string]*file.FileDescriptor // key: root path, value: fd, each fd must be independent of other
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

func (fs *FileServer) Mount(req proto.MountRequest, resp *proto.MountResponse) error {
	log.Printf("FileServer.Mount is called")
	// every filepath is found through root + path for security
	root, path, err := fs.find(req.File)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	rootfd := fs.trees[root]
	fd := file.Search(rootfd, path)
	if fd == nil {
		return fmt.Errorf("file server: no such file") // should never happen
	}
	// TODO: add subscribers
	fd.Subscribers[req.ClientId] = true
	resp.Fd = fd
	return nil
}

func (fs *FileServer) Create(req proto.CreateRequest, resp *proto.CreateResponse) error {
	log.Printf("FileServer.Create is called")
	root, _, err := fs.find(filepath.Dir(req.FilePath))
	if err != nil {
		return err
	}
	// check if the parent directory exists
	parentDir := filepath.Dir(req.FilePath)
	if _, err := os.Stat(filepath.Join(root, parentDir)); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("file server: dir %s does not exist", parentDir)
	}
	rootfd := fs.trees[root]
	pfd := file.Search(rootfd, parentDir)
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
		fd := &file.FileDescriptor{
			FilePath:    req.FilePath,
			IsDir:       false,
			Owner:       req.ClientId,
			Subscribers: make(map[string]bool),
		}
		// add to tree
		pfd.AddChild(fd)
		resp.Fd = fd
		return nil
	}
	return fmt.Errorf("file server: %s already exists", filepath.Base(req.FilePath))
}

func (fs *FileServer) MkDir(req proto.CreateRequest, resp *proto.CreateResponse) error {
	log.Printf("FileServer.MkDir is called")
	req.FilePath = strings.TrimPrefix(req.FilePath, string(os.PathSeparator))
	parts := strings.Split(req.FilePath, string(os.PathSeparator))
	root, _, err := fs.find(parts[0])
	if err != nil {
		return err
	}
	pfd := fs.trees[root]
	since := -1
	fds := make([]*file.FileDescriptor, 0)
	for i, _ := range parts {
		path := filepath.Join(parts[:i+1]...)
		localDir := filepath.Join(root, path)
		// check if the directory exists
		if _, err := os.Stat(localDir); os.IsNotExist(err) {
			if since == -1 {
				since = i
			}
			if err := os.Mkdir(localDir, os.ModePerm); err != nil {
				return fmt.Errorf("file server: error creating directory %s, err: %v", path, err)
			}
			fd := &file.FileDescriptor{
				FilePath:    path,
				IsDir:       true,
				Owner:       req.ClientId,
				Subscribers: make(map[string]bool),
			}
			fds = append(fds, fd)
			pfd.AddChild(fd)
			pfd = fd
		} else {
			//exists
			pfd = pfd.FindChildWithFilePath(path)
			fds = append(fds, pfd)
		}
	}
	if since == -1 {
		return fmt.Errorf("file server: dir: %s exists", req.FilePath)
	}
	topfd := fds[since]
	topfd.Subscribers[req.ClientId] = true
	resp.Fd = topfd
	return nil
}

func (fs *FileServer) RmDir(req proto.RemoveRequest, resp *proto.RemoveResponse) error {
	log.Printf("FileServer.RmDir is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
	if err := os.RemoveAll(localPath); err != nil{
		resp.IsRemoved = false
		return fmt.Errorf("file server: remove %s error: %v", req.FilePath, err)
	}
	// update on tree
	rootfd := fs.trees[root]
	pfd := file.Search(rootfd, filepath.Dir(req.FilePath))
	pfd.UpdateAllSubscribers()
	pfd.RemoveChild(req.FilePath)
	resp.IsRemoved = true
	return nil
}

func (fs *FileServer) Read(req proto.ReadRequest, resp *proto.ReadResponse) error {
	log.Printf("FileServer.Read is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("file server: open error: %v", err)
	}
	defer f.Close()
	buf := make([]byte, req.N)
	if _, err = f.ReadAt(buf, req.Offset); err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	resp.Content = append(resp.Content, buf...)
	return nil
}

func (fs *FileServer) Write(req proto.WriteRequest, resp *proto.WriteResponse) error {
	log.Printf("FileServer.Write is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	localPath := filepath.Join(root, req.FilePath)
	f1, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
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
	fd := file.Search(fs.trees[root], localPath)
	fd.UpdateAllSubscribers()
	resp.N = int64(n)
	return nil
}

func (fs *FileServer) Remove(req proto.RemoveRequest, resp *proto.RemoveResponse) error {
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
	rootfd := fs.trees[root]
	pfd := file.Search(rootfd, filepath.Dir(req.FilePath))
	pfd.UpdateAllSubscribers()
	pfd.RemoveChild(req.FilePath)
	resp.IsRemoved = true
	return nil
}

func NewFileServer(config *config.Config) *FileServer {
	exportedRootPaths := os.Getenv("EXPORT_ROOT_PATHS")
	paths := strings.Split(exportedRootPaths, ":")
	fs := &FileServer{
		addr:              config.ServerAddr,
		exportedRootPaths: paths,
		trees:             make(map[string]*file.FileDescriptor),
	}
	if err := rpc.Register(fs); err != nil {
		log.Fatal("rpc register error:", err)
	}
	for _, path := range paths {
		fs.trees[path] = buildTree(path)
	}
	return fs
}

func buildTree(entry string) *file.FileDescriptor {
	if entry == "" {
		panic("no exported directories")
	}
	fi, _ := os.Stat(entry)
	entryfd := &file.FileDescriptor{
		FilePath:    "",
		IsDir:       fi.IsDir(),
		Size:        fi.Size(),
		Children:    make([]*file.FileDescriptor, 0),
		Subscribers: make(map[string]bool),
	}
	parents := make(map[string]*file.FileDescriptor)
	parents[entry] = entryfd
	err := filepath.Walk(entry, func(currentPath string, info os.FileInfo, err error) error {
		if currentPath == entry {
			return nil
		}
		pfd := parents[filepath.Dir(currentPath)]
		cfd := &file.FileDescriptor{
			FilePath:    strings.TrimPrefix(currentPath, entry),
			IsDir:       info.IsDir(),
			Size:        info.Size(),
			Children:    make([]*file.FileDescriptor, 0),
			Subscribers: make(map[string]bool),
		}
		pfd.Children = append(pfd.Children, cfd)
		if _, ok := parents[currentPath]; !ok {
			parents[currentPath] = cfd
		}
		return nil
	})

	if err != nil {
		panic(fmt.Sprintf("file server build tree error: %v", err))
	}
	return entryfd
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
