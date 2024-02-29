package service

import (
	"fmt"
	"path/filepath"
	"strings"
)

type Volume struct {
	fstype FileSystemType
	root   *FileDescriptor // root of the volume
	cache  *Cache
}

func NewVolume(root *FileDescriptor, fstype FileSystemType) *Volume {
	return &Volume{root: root, fstype: fstype, cache: NewCache()}
}

type FileDescriptor struct {
	IsDir        bool
	FilePath     string // server-side path to the file
	Owner        string // owner of the file
	Seeker       uint64 // last seek position, only used at client side
	Children     []*FileDescriptor
	sub          *Subscription // list of client ids that are subscribe to this file descriptor
	lastModified int64         // last modification time in unix time
}

func NewFileDescriptor(isdir bool, filepath string) *FileDescriptor {
	return &FileDescriptor{
		IsDir:    isdir,
		FilePath: filepath,
		Children: make([]*FileDescriptor, 0),
		sub:      NewSubscription(),
	}
}

// function to print file tree starting from root
func PrintTree(prefix string, root *FileDescriptor) {
	print(prefix, root)
}

// find the matching file descriptor by searching from the tree root
// the given file string must remove the server root path
func Search(root *FileDescriptor, file string) *FileDescriptor {
	if root == nil {
		return nil
	}
	if strings.HasSuffix(root.FilePath, file) {
		return root
	}
	var found *FileDescriptor
	for _, cfd := range root.Children {
		found = Search(cfd, file)
		if found != nil {
			return found
		}
	}
	return found
}

// remove from the tree rooted at `root`
func RemoveFromTree(root *FileDescriptor, file string) {
	parent := filepath.Dir(file) // parent filepath
	pfd := Search(root, parent)
	pfd.RemoveChild(file)
}

// add fd to the tree rooted at the `root`
func AddToTree(root *FileDescriptor, fd *FileDescriptor) {
	parent := filepath.Dir(fd.FilePath) // parent filepath
	pfd := Search(root, parent)
	pfd.AddChild(fd)
}

func (fd *FileDescriptor) AddChild(child *FileDescriptor) {
	fd.Children = append(fd.Children, child)
}

func (fd *FileDescriptor) RemoveChild(file string) {
	idx := fd.FindChildIndex(file)
	if idx != -1 {
		fd.Children = append(fd.Children[:idx], fd.Children[idx+1:]...)
	}
}

func (fd *FileDescriptor) FindChildWithFilePath(file string) *FileDescriptor {
	for _, cfd := range fd.Children {
		if strings.HasSuffix(cfd.FilePath, file) {
			return cfd
		}
	}
	return nil
}

func (fd *FileDescriptor) FindChildIndex(file string) int {
	for idx, cfd := range fd.Children {
		if strings.HasSuffix(cfd.FilePath, file) {
			return idx
		}
	}
	return -1
}

func print(rootpath string, fd *FileDescriptor) {
	fmt.Printf("\t%s\n", filepath.Join(rootpath, fd.FilePath))
	for _, cfd := range fd.Children {
		print(rootpath, cfd)
	}
}

// recursively performs subscription
func Subscribe(root *FileDescriptor, clientId, clientAddr string) {
	if root == nil {
		return
	}
	root.sub.Subscribe(clientId, clientAddr)
	for _, cfd := range root.Children {
		Subscribe(cfd, clientId, clientAddr)
	}
}

func Unsubscribe(root *FileDescriptor, clientId string) {
	if root == nil {
		return
	}
	root.sub.Unsubscribe(clientId)
	for _, cfd := range root.Children {
		Unsubscribe(cfd, clientId)
	}
}
