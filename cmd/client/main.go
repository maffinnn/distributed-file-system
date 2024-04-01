package main

import (
	"bufio"
	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	id := flag.String("clientId", "1", "ID of the client")
	addr := flag.String("addr", ":8081", "client address")
	serverAddr := flag.String("serverAddr", ":8080", "server address to connect to")
	networkPacketLossProbability := flag.Int("networkLossProb", 0, "")
	flag.Parse()
	rpc.ClientSideNetworkPacketLossProbability = *networkPacketLossProbability
	c := service.NewFileClient(*id, *addr, *serverAddr)
	fmt.Println(*id, *addr, *serverAddr)
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
		case "touch":
		case "ls":
			c.ListAllFiles()
		case "help":
		case "quit":
			fmt.Printf("Exiting the program...")
			return
		}
		fmt.Printf("> ")
	}
}
