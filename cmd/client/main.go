package main

import (
	"bufio"
	"distributed-file-system/lib/golang/service"
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
	flag.Parse()
	c := service.NewFileClient(*id, *addr, *serverAddr)
	fmt.Println(*id, *addr, *serverAddr)
	go c.Run()

	fmt.Printf("Starting file client %s...\n", *id)
	fmt.Printf("> ")
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
		case "mount": // mount path/to/source/dir/at/server path/to/target/dir/at/client --filesystemtype=e.g.AFS,SNFS --duration(in seconds)=30 --pollInterval(in miliseconds)=10
			if len(words) < 5 {
				fmt.Printf("ERROR: invalid input")
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
			} else if len(words) != 6 {
				fmt.Printf("ERROR: Sun Network File System is chosen but freshness interval is set.\n")
				continue
			} else {
				fs = service.SunNetworkFileSystemType
				// if user opts to sun network file system, user also nees to specify the freshness interval for cache consistency
				// otherwise a default value of 30 seconds would be used
				service.PollInterval, _ = strconv.Atoi(strings.TrimSpace(words[5]))
			}
			service.Duration, err = strconv.Atoi(strings.TrimSpace(words[4]))
			if err != nil {
				fmt.Printf("ERROR: %v", err)
				continue
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
				fmt.Printf("ERROR: invalid inpu\n")
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
