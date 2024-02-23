package file

type FileDescriptor struct {
	Fsid uint64 // file system id
	Fid uint64 // file id
	Dir string
	Name string
	Attr string
}
