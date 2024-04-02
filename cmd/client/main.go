package main

import (
	"bufio"
	"distributed-file-system/pkg/golang/config"
	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var serverAddr = ":8080"

func main() {

	id := flag.String("id", "1", "id of the client")
	addr := flag.String("addr", ":8081", "address of the client")
	s := flag.String("setting", "AtLeastOnceIdempotent", "")
	flag.Parse()

	settings := map[string]config.Config{
		"AtLeastOnceIdempotent": {
			ClientSideNetworkPacketLossProbability: 20,
			RpcTimeout:                             100 * time.Millisecond,
		},
		"AtLeastOnceNonIdempotent": {
			ClientSideNetworkPacketLossProbability: 0,
			RpcTimeout:                             750 * time.Millisecond,
		},
		"AtMostOnceIdempotent": {
			ClientSideNetworkPacketLossProbability: 10,
			RpcTimeout:                             200 * time.Millisecond,
		},
		"AtMostOnceNonIdempotentRead": {
			ClientSideNetworkPacketLossProbability: 0,
			RpcTimeout:                             800 * time.Millisecond,
		},
		"TestCacheConsistency": {
			ClientSideNetworkPacketLossProbability: 0,
			RpcTimeout:                             100 * time.Millisecond,
		},
	}

	if conf, ok := settings[*s]; ok {
		rpc.ClientSideNetworkPacketLossProbability = conf.ClientSideNetworkPacketLossProbability
		rpc.Timeout = conf.RpcTimeout
	} else {
		fmt.Printf("error flag")
		return
	}

	c := service.NewFileClient(*id, *addr, serverAddr)
	go c.Run()

	fmt.Printf("Starting file client %s...\n> ", *id)
	scanner := bufio.NewScanner(os.Stdin)
	var err error
	var fd *service.FileDescriptor
	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, " ")
		if len(words) <= 0 {
			fmt.Printf("ERROR: invalid input")
			continue
		}
		cmd := strings.TrimSpace(words[0])
		switch cmd {
		case "mount": // mount path/to/source/dir/at/server path/to/target/dir/at/client filesystemtype=e.g.AFS,SNFS
			if len(words) != 4 {
				fmt.Printf("ERROR: invalid arguement")
				continue
			}
			srcDir := strings.TrimSpace(words[1])
			targetDir := strings.TrimSpace(words[2])
			fstype := strings.TrimSpace(words[3])
			var fs service.FileSystemType
			if fstype != "AFS" && fstype != "SNFS" {
				fmt.Printf("ERROR: invalid file system type")
				continue
			}
			if fstype == "AFS" {
				fs = service.AndrewFileSystemType
			} else {
				fs = service.SunNetworkFileSystemType
			}
			c.Mount(srcDir, targetDir, fs)
		case "open":
			if len(words) != 2 {
				fmt.Printf("ERROR: invalid input\n")
				continue
			}
			localDir := strings.TrimSpace(words[1])
			fd, err = c.Open(localDir)
			if err != nil {
				fmt.Printf("ERROR: %v\n", err)
				continue
			}
		case "readi": // idempotent read
			if fd == nil {
				fmt.Printf("ERROR: file not opened\n")
				continue
			}
			offset, _ := strconv.Atoi(words[1])
			n, _ := strconv.Atoi(words[2])
			c.ReadAt(fd, offset, n)
		case "read": // non idempotent read
			if fd == nil {
				fmt.Printf("ERROR: file not opened\n")
				continue
			}
			n, _ := strconv.Atoi(words[1])
			c.Read(fd, n)
		case "write":
			if len(words) != 3 {
				fmt.Printf("ERROR: invalid input\n")
				continue
			}
			if fd == nil {
				fmt.Printf("ERROR: file not opened\n")
				continue
			}
			offset, _ := strconv.Atoi(words[1])
			data := []byte(words[2])
			n, err := c.Write(fd, offset, data)
			if err != nil {
				fmt.Printf("ERROR: %v\n", err)
			}
			fmt.Printf("%d bytes written to %s\n", n, fd.Filepath)
		case "ls":
			c.ListAllFiles()
		case "quit":
			fmt.Printf("Exiting the program...")
			return
		}
		fmt.Printf("> ")
	}
}
