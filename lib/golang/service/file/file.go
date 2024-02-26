package file

import (
	"fmt"
	"strings"
	"path/filepath"
)

type FileDescriptor struct {
	IsDir bool
	FilePath string // server-side path to the file
	Owner string // owner of the file
	FileMode uint32 // access mode
	Size  int64 // size of the file
	Seeker uint64 // last seek position, only used at client side
	Parent *FileDescriptor
	Children []*FileDescriptor
	subscribers map[string]bool // only used at server side
}

func Print(rootpath string, fd *FileDescriptor) {
	topLevelPrefix := fd.FilePath
	fmt.Printf("local file:\n")
	print(rootpath, topLevelPrefix, fd)
}

func print(rootpath, prefix string, fd *FileDescriptor) {
	fmt.Printf("\t%s\n", filepath.Join(rootpath, strings.TrimPrefix(fd.FilePath, prefix)))
	for _, cfd := range fd.Children {
		print(rootpath, prefix, cfd)
	}
}

// find the matching file descriptor given the root
// the given file string must remove the server root path
func findFd(file string, root *FileDescriptor) *FileDescriptor{
	if root == nil {
		return nil
	}
	if strings.HasSuffix(root.FilePath, file) {
		return root
	}
	var found *FileDescriptor
	for _, cfd := range root.Children {
		found = Find(file, cfd)
	}
	return found
}

func (fd *FileDescriptor) RemoveChild(child *FileDescriptor) {
	idx := fd.FindChildIndex(child)
	if idx != -1 {
		fd.Children = append(fd.Children[:index], fd.Children[idx+1:]...)
	}
}

func (fd *FileDescriptor) FindChildWithFilePath(filepath string) *FileDescriptor {
	for idx, cfd := range fd.Children {
		if fd.FilePath == filepath {
			return cfd
		}
	}
	return nil
}

func (fd *FileDescriptor) FindChildWithChildFd(child *FileDescriptor) *FileDescriptor {
	for idx, cfd := range fd.Children {
		if fd.FilePath == child.FilePath {
			return cfd
		}
	}
	return nil
}

func (fd *FileDescriptor) FindChildIndex(child *FileDescriptor) int {
	for idx, cfd := range fd.Children {
		if fd.FilePath == child.FilePath {
			return idx
		}
	}
	return -1
}

func (fd *FileDescriptor) UpdateAllSubscribers() {
	// TODO
}