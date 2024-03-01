package service

import "time"

type FileSystemType string

type FileSystemConfig struct {
	fstype       FileSystemType
	duration     time.Duration
	pollInterval time.Duration
}

const (
	SunNetworkFileSystemType FileSystemType = "SunNetworkFileSystem" //NFS
	AndrewFileSystemType     FileSystemType = "AndrewFileSystem"     //AFS
)
