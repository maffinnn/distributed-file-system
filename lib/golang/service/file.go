package service

import (
	"fmt"
	"path/filepath"
	fp "path/filepath"
	"strings"
)

type Volume struct {
	fstype FileSystemType
	root   *FileDescriptor // root of the volume
}

func NewVolume(root *FileDescriptor, fstype FileSystemType) *Volume {
	return &Volume{root: root, fstype: fstype}
}

type FileDescriptor struct {
	IsDir           bool
	Filepath        string // server-side path to the file
	owner           string // owner of the file
	Seeker          uint64 // last seek position, only used at client side
	Children        []*FileDescriptor
	subscription    *Subscription    // list of client ids that are subscribe to this file descriptor
	LastModified    int64            // last modification time in unix time
	CallbackPromise *CallbackPromise // callback promise for andrew filesystem, used at client side
}

func NewFileDescriptor(isDir bool, filepath string) *FileDescriptor {
	return &FileDescriptor{
		IsDir:        isDir,
		Filepath:     filepath,
		Seeker:       0,
		Children:     make([]*FileDescriptor, 0),
		subscription: NewSubscription(),
	}
}

// function to print file tree starting from root
func PrintTree(prefix string, root *FileDescriptor) {
	print(prefix, root)
}

// find the matching file descriptor by searching from the tree root
// the given file string must remove the server root path
func Search(root *FileDescriptor, filepath string) *FileDescriptor {
	if root == nil {
		return nil
	}
	if strings.HasSuffix(root.Filepath, filepath) {
		return root
	}
	var found *FileDescriptor
	for _, cfd := range root.Children {
		found = Search(cfd, filepath)
		if found != nil {
			return found
		}
	}
	return found
}

// remove from the tree rooted at `root`
func RemoveFromTree(root *FileDescriptor, filepath string) {
	parent := fp.Dir(filepath) // parent filepath
	pfd := Search(root, parent)
	pfd.RemoveChild(filepath)
}

// add fd to the tree rooted at the `root`
func AddToTree(root *FileDescriptor, fd *FileDescriptor) {
	parent := fp.Dir(fd.Filepath) // parent filepath
	pfd := Search(root, parent)
	pfd.AddChild(fd)
}

func (fd *FileDescriptor) AddChild(child *FileDescriptor) {
	fd.Children = append(fd.Children, child)
}

func (fd *FileDescriptor) RemoveChild(filepath string) {
	idx := fd.FindChildIndex(filepath)
	if idx != -1 {
		fd.Children = append(fd.Children[:idx], fd.Children[idx+1:]...)
	}
}

func (fd *FileDescriptor) FindChildWithFilePath(filepath string) *FileDescriptor {
	for _, cfd := range fd.Children {
		if strings.HasSuffix(cfd.Filepath, filepath) {
			return cfd
		}
	}
	return nil
}

func (fd *FileDescriptor) FindChildIndex(filepath string) int {
	for idx, cfd := range fd.Children {
		if strings.HasSuffix(cfd.Filepath, filepath) {
			return idx
		}
	}
	return -1
}

func print(rootpath string, fd *FileDescriptor) {
	fmt.Printf("\t%s\n", filepath.Join(rootpath, fd.Filepath))
	for _, cfd := range fd.Children {
		print(rootpath, cfd)
	}
}

// recursively performs subscription
func Subscribe(root *FileDescriptor, clientId, clientAddr string) {
	if root == nil {
		return
	}
	root.subscription.Subscribe(clientId, clientAddr)
	for _, cfd := range root.Children {
		Subscribe(cfd, clientId, clientAddr)
	}
}

func Unsubscribe(root *FileDescriptor, clientId string) {
	if root == nil {
		return
	}
	root.subscription.Unsubscribe(clientId)
	for _, cfd := range root.Children {
		Unsubscribe(cfd, clientId)
	}
}
