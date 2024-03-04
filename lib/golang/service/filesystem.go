package service

type FileSystemType string

const (
	SunNetworkFileSystemType FileSystemType = "SunNetworkFileSystem" //NFS
	AndrewFileSystemType     FileSystemType = "AndrewFileSystem"     //AFS
)
