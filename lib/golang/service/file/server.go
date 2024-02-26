package file

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"errors"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/rpc"
	"distributed-file-system/lib/golang/service/proto"
)

type Server struct {
	addr string
	exportedRootPaths []string // top level directory path that the server is exporting
	trees map[string]*FileDescriptor // key: root path, value: fd, each fd must be independent of other
}

// return the root, file path since the root, and error
func (fs *Server) find(file string) (string, string, error) {
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

func (fs *Server) Mount(req proto.MountRequest, resp *proto.MountResponse) error {
	log.Printf("FileServer.Mount is called")
	// every filepath is found through root + path for security
	root, path, err := fs.find(req.File)
	if err != nil {
		return fmt.Errorf("file server: %v", err)
	}
	rootfd := trees[root]
	fd := findFd(path, rootfd)
	if fd == nil {
		return fmt.Errorf("file server: no such file") // should never happen
	}
	// TODO: add subscribers
	fd.subscribers[req.ClientId] = true
	resp.Fd = fd
	return nil
}

func (fs *Server) Create(req proto.CreateRequest, resp *proto.CreateResponse) error {
	log.Printf("FileServer.Create is called")
	root, path, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}
	// check if the parent directory exists
	parentDir := filepath.Dir(req.FilePath)
	if _, err := os.Stat(filepath.Join(root, parentDir)); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("file server: dir %s does not exist", parentDir)
	}
	rootfd := trees[root]
	parentfd := findFd(parentDir, rootfd)
	if parentfd == nil {
		return fmt.Errorf("file server: dir %s does not exist", parentDir)
	}
	// check if the file exists
	localPath := filepath.Join(root, req.FilePath)
	if _, err := os.Stat(localPath); errors.Is(err, os.ErrNotExist) {
		_, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("file server: create error %v", err)
		}
		fd := &FileDescriptor{
			FilePath: req.FilePath,
			IsDir: false,
			Owner: req.ClientId,
			Parent: parentfd,
			subscribers: make(map[string]bool),
		}
		// add to tree
		parentfd.Children = append(parentfd.Children, fd)
		resp.Fd = fd
		return nil
	}
	return fmt.Errorf("file server: %s already exists", filepath.Base(req.FilePath))
}


func (fs *Server) MkDir(req proto.CreateRequest, resp *proto.CreateResponse) error {
	log.Printf("FileServer.MkDir is called")
	root, _, err := fs.find(req.FilePath)
	if err != nil {
		return err
	}

	pfd := trees[root]
	parts := strings.Split(req.FilePath, string(os.PathSeparator))
	since := -1
	fds := make([]*FileDescriptor, 0)
	for i := range len(parts) {
		path := filepath.Join(parts[:i+1]...)
		localDir := filepath.Join(root, path)
		// check if the directory exists
		if _, err := os.Stat(localDir); os.IsNotExist(err) {
			if since == -1 {
				since = i
			}
			if err := os.Mkdir(localDir, os.ModePerm); err != nil {
				return err
			}
			fd := &FileDescriptor{
				FilePath: path,
				IsDir: true,
				Owner: req.ClientId,
				Parent: pfd,
				subscribers: make(map[string]bool),
			}
		}else {
			//exists
			pfd = pfd.FindChildWithFilePath(path)
		}
		fds = append(fds, pfd)
	}
	topfd := fds[since]
	topfd.subscribers[req.ClientId] = true
	resp.Fd = topfd
	return nil
}

func (fs *Server) Read(req proto.ReadRequest, resp *proto.ReadResponse) error {
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


func (fs *Server) Write(req proto.WriteRequest, resp *proto.WriteResponse) error {
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
	fd := findFd(localPath, trees[root])
	fd.UpdateAllSubscribers()
	resp.N = int64(n)
	return nil
}

func (fs *Server) Remove(req proto.RemoveRequest, resp *proto.RemoveResponse) error {
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
	rootfd := trees[root]
	fd := findFd(localPath, rootfd)
	parentfd := fd.Parent
	fd.UpdateAllSubscribers()
	parentfd.Remove(fd)
	resp.IsRemoved = true
	return nil
}

func NewFileServer(config *config.Config) *Server {
	exportedRootPaths := os.Getenv("EXPORT_ROOT_PATHS")
	paths := strings.Split(exportedRootPaths, ":")
	fs := &Server{
		addr: config.ServerAddr, 
		exportedRootPaths: paths,
		trees: make(map[string]*FileDescriptor),
	}
	if err := rpc.Register(fs); err != nil {
		log.Fatal("rpc register error:", err)
	}
	for _, path := range paths {
		fs.trees[path] = buildTree(path)
	}
	return fs
}

func buildTree(entry string) *FileDescriptor {
	fi, _ := os.Stat(entry)
	entryfd := &FileDescriptor{
		FilePath: "",
		IsDir: fi.IsDir(),
		Size:  fi.Size(),
		Children: make([]*FileDescriptor, 0),
		subscribers: make(map[string]bool),
	}
	parents := make(map[string]*FileDescriptor)
	parent[entry] = entryfd
	err = filepath.Walk(entry, func(currentPath string, info os.FileInfo, err error) error {
		if currentPath == entry {
			return nil
		}
		pfd := parent[filepath.Dir(currentPath)]
		cfd := &FileDescriptor{
			FilePath: strings.TrimPrefix(currentPath, entry),
			IsDir: info.IsDir(),
			Size:  info.Size(),
			Parent: pfd,
			Children: make([]*FileDescriptor, 0),
			subscribers: make(map[string]bool),
		}
		pfd.Children = append(pfd.Children, cfd)
		if _, ok := parent[currentPath]; !ok {
			parent[currentPath] = cfd
		}
		return nil
	})
	return entryfd
}


func (fs *Server) Run() {
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
