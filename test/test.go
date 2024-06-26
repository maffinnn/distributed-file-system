package main

import (
	"fmt"
	"time"

	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
)

var serverAddr string = ":8080"
var timeFormat string = "2006-01-02 15:04:05"

func SimpleTest() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	// server := service.NewFileServer(serverAddr)
	// go server.Run()
	time.Sleep(5 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	src := "etc/exports/mockdir1"
	target := "1"
	fmt.Printf("Mounting directory from server directory %s to local directory %s...\n", src, target)
	c1.Mount(src, target, service.AndrewFileSystemType)

	c1.ListFiles(target)

	newFile := "1/subdir1/testcreate1.txt"
	fmt.Printf("\nCreating file %s...\n\n", newFile)
	fd, err := c1.Create(newFile)
	if err != nil {
		fmt.Printf("create error %v", err)
		return
	}

	time.Sleep(5 * time.Second)

	txtToWrite := []byte("test create file\nEOF")
	writeAt := 0
	fmt.Printf("Writing text to file %s at position %d...\n", newFile, writeAt)
	n, err := c1.Write(fd, writeAt, txtToWrite)
	if err != nil {
		fmt.Printf("write error %v", err)
		return
	}
	fmt.Printf("%d bytes written to %s\n\n", n, "1/subdir1/testcreate1.txt")

	time.Sleep(5 * time.Second)
	fmt.Printf("Reading 1/subdir1/testcreate1.txt...\n")
	data, err := c1.ReadAt(fd, 0, 1000)
	if err != nil {
		fmt.Printf("read error %v", err)
		return
	}
	fmt.Printf("%s\n", string(data))

	time.Sleep(5 * time.Second)

	writeAt = 5
	fmt.Printf("Writing text to file %s at position %d...\n", newFile, writeAt)
	n, err = c1.Write(fd, writeAt, []byte(fmt.Sprintf("test write file at position %d[whitespace]", writeAt)))
	if err != nil {
		fmt.Printf("write error %v", err)
		return
	}
	fmt.Printf("%d bytes written to %s\n\n", n, "1/subdir1/testcreate1.txt")

	time.Sleep(5 * time.Second)

	fmt.Printf("Reading 1/subdir1/testcreate1.txt at position %d...\n", writeAt)
	data, err = c1.ReadAt(fd, writeAt, 1000)
	if err != nil {
		fmt.Printf("read error %v", err)
		return
	}
	fmt.Printf("%s\n", string(data))
	c1.Close(fd)
}

func senario1(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fmt.Printf("[file client 1] Open session on file 1/testfile3.txt\n")
	fdC1, err := c1.Open("1/testfile3.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	time.Sleep(2 * time.Second)

	// client 1 reads the file
	fmt.Printf("[file client 1] Reading 1/testfile3.txt...\n")
	data, err := c1.ReadAt(fdC1, 0, 1000) // read all the content
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))
	c1.Close(fdC1)
	fmt.Printf("[file client 1] Close session on file 1/testfile3.txt\n")

	time.Sleep(2 * time.Second)

	// client 2 opens the file
	fmt.Printf("[file client 2] Open session on file 2/testfile3.txt\n")
	fdC2, err = c2.Open("2/testfile3.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	time.Sleep(2 * time.Second)

	// client 2 updates the file
	fmt.Printf("[file client 2] Reading 2/testfile3.txt...\n")
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))

	time.Sleep(2 * time.Second)

	fmt.Printf("[file client 2] Writing text to file 2/testfile3.txt at position 0...\n")
	n, err := c2.Write(fdC2, 0, []byte(fmt.Sprintf("write to file at %s by client 2\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("[file client 2] %d bytes are written to %s\n", n, "2/testfile3.txt")

	time.Sleep(2 * time.Second)

	fmt.Printf("[file client 2] Reading 2/testfile3.txt...\n")
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))
	c2.Close(fdC2)
	fmt.Printf("[file client 2] Close session on file 2/testfile3.txt\n")

	time.Sleep(2 * time.Second)

	// client 1 reads the file
	fmt.Printf("[file client 1] Open session on file 1/testfile3.txt\n")
	fdC1, err = c1.Open("1/testfile3.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	time.Sleep(2 * time.Second)

	fmt.Printf("[file client 1] Reading 1/testfile3.txt...\n")
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))

	time.Sleep(2 * time.Second)

	// client 2 updates the file again
	fmt.Printf("[file client 2] Writing text to file 2/testfile3.txt at position 0...\n")
	n, err = c2.Write(fdC2, 0, []byte(fmt.Sprintf("write to file at %s by client 2 again\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("[file client 2] %d bytes are written to %s\n", n, "2/testfile3.txt")
	c2.Close(fdC2)
	fmt.Printf("[file client 2] Close session on file 2/testfile3.txt\n")

	time.Sleep(2 * time.Second)

	// client 1 reads the file
	fmt.Printf("[file client 1] Reading 1/testfile3.txt...\n")
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))
	c1.Close(fdC1)
	fmt.Printf("[file client 1] Close session on file 1/testfile3.txt\n")
}

func senario2(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fmt.Printf("[file client 1] Open session on file 1/testfile2.txt\n")
	fdC1, err := c1.Open("1/testfile2.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	time.Sleep(2 * time.Second)

	// client 1 reads
	fmt.Printf("[file client 1] Reading 1/testfile2.txt...\n")
	data, err := c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))

	time.Sleep(2 * time.Second)

	// client 2 opens the file
	fmt.Printf("[file client 2] Open session on file 2/testfile2.txt\n")
	fdC2, err = c2.Open("2/testfile2.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	// client 2 reads
	fmt.Printf("[file client 2] Reading 2/testfile2.txt...\n")
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))

	time.Sleep(2 * time.Second)

	// client 2 updates the file and close
	fmt.Printf("[file client 2] Writing text to file 2/testfile2.txt at position 0...\n")
	n, err := c2.Write(fdC2, 0, []byte(fmt.Sprintf("write to file at %s by client 2\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("[file client 2] %d bytes are written to %s\n", n, "2/testfile2.txt")

	fmt.Printf("[file client 2] Reading 2/testfile2.txt...\n")
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))
	c2.Close(fdC2)
	fmt.Printf("[file client 2] Close session on file 2/testfile2.txt\n")

	time.Sleep(2 * time.Second)

	// client 1 updates the file
	fmt.Printf("[file client 1] Writing text to file 1/testfile2.txt at position 0...\n")
	n, err = c1.Write(fdC1, 0, []byte(fmt.Sprintf("write to file at %s by client 1\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("[file client 1] %d bytes are written to %s\n", n, "1/testfile2.txt")

	time.Sleep(2 * time.Second)

	// client 1 reads the file
	fmt.Printf("[file client 1] Reading 1/testfile2.txt...\n")
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("%s\n", string(data))
	c1.Close(fdC1)
	fmt.Printf("[file client 1] Close session on file 1/testfile2.txt\n")
}

func TestCacheConsistencyAFSSenario1() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	// server := service.NewFileServer(serverAddr)
	// go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()

	src := "etc/exports/mockdir1"
	target1 := "1"
	target2 := "2"
	fmt.Printf("[file client 1] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: Session Update...\n", src, target1)
	c1.Mount(src, target1, service.AndrewFileSystemType)
	c1.ListFiles(target1)

	fmt.Printf("\n[file client 2] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: Session Update...\n", src, target2)
	c2.Mount(src, target2, service.AndrewFileSystemType)
	c2.ListFiles(target2)
	fmt.Printf("\n")

	senario1(c1, c2)
}

func TestCacheConsistencyAFSSenario2() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	// server := service.NewFileServer(serverAddr)
	// go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()

	src := "etc/exports/mockdir1"
	target1 := "1"
	target2 := "2"
	fmt.Printf("[file client 1] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: Session Update...\n", src, target1)
	c1.Mount(src, target1, service.AndrewFileSystemType)
	c1.ListFiles(target1)

	fmt.Printf("\n[file client 2] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: Session Update...\n", src, target2)
	c2.Mount(src, target2, service.AndrewFileSystemType)
	c2.ListFiles(target2)
	fmt.Printf("\n")

	senario2(c1, c2)
}

func TestCacheConsistencyNFSSenario1() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	service.PollInterval = 100 // in milliseconds

	// server := service.NewFileServer(serverAddr)
	// go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()

	src := "etc/exports/mockdir1"
	target1 := "1"
	target2 := "2"
	fmt.Printf("[file client 1] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: One-Copy Update...\n", src, target1)
	c1.Mount(src, target1, service.SunNetworkFileSystemType)
	c1.ListFiles(target1)

	fmt.Printf("\n[file client 2] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: One-Copy Update...\n", src, target2)
	c2.Mount(src, target2, service.SunNetworkFileSystemType)
	c2.ListFiles(target2)
	fmt.Printf("\n")
	senario1(c1, c2)
}

func TestCacheConsistencyNFSSenario2() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond
	service.PollInterval = 100 // in milliseconds

	// server := service.NewFileServer(serverAddr)
	// go server.Run()

	time.Sleep(2 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()

	src := "etc/exports/mockdir1"
	target1 := "1"
	target2 := "2"
	fmt.Printf("[file client 1] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: One-Copy Update...\n", src, target1)
	c1.Mount(src, target1, service.SunNetworkFileSystemType)
	c1.ListFiles(target1)

	fmt.Printf("\n[file client 2] Mounting directory from server directory %s to local directory %s with cache consistency mechanism: One-Copy Update...\n", src, target2)
	c2.Mount(src, target2, service.SunNetworkFileSystemType)
	c2.ListFiles(target2)
	fmt.Printf("\n")

	senario2(c1, c2)
}

func performIdempotentRead() {
	// server := service.NewFileServer(serverAddr)
	// go server.Run()

	time.Sleep(2 * time.Second) // to make sure server is up

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	time.Sleep(2 * time.Second)

	src := "etc/exports/mockdir1/subdir3/testidempotent.txt"
	target := "localfile/1/testidempotent.txt"
	fmt.Printf("Mounting directory from server directory %s to local directory %s...\n", src, target)
	err := c1.Mount(src, target, service.SunNetworkFileSystemType)
	if err != nil {
		fmt.Printf("mount error: %v\n", err)
		return
	}

	time.Sleep(2 * time.Second)
	localPath := "localfile/1/testidempotent.txt"
	fd, err := c1.Open(localPath)
	if err != nil {
		fmt.Printf("open testidempotent.txt error: %v\n", err)
		return
	}

	time.Sleep(2 * time.Second)

	nToRead := 50
	readAt := 0
	fmt.Printf("Reading %s for %d bytes at position %d...\n", localPath, nToRead, readAt)

	time.Sleep(2 * time.Second)

	data, err := c1.ReadAt(fd, readAt, nToRead)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("Read file content: \n%s\n", string(data))
}

func performNonIdempotentRead() {
	// init server
	// server := service.NewFileServer(serverAddr)
	// go server.Run()

	time.Sleep(2 * time.Second) // to make sure server is up

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	time.Sleep(2 * time.Second)

	src := "etc/exports/mockdir1/subdir3/testidempotent.txt"
	target := "localfile/1/testidempotent.txt"
	fmt.Printf("Mounting directory from server directory %s to local directory %s...\n", src, target)
	err := c1.Mount(src, target, service.SunNetworkFileSystemType)
	if err != nil {
		fmt.Printf("mount error: %v\n", err)
		return
	}

	time.Sleep(2 * time.Second)

	localPath := "localfile/1/testidempotent.txt"
	fd, err := c1.Open(localPath)
	if err != nil {
		fmt.Printf("open testidempotent.txt error: %v\n", err)
		return
	}

	time.Sleep(2 * time.Second)
	nToRead := 50
	// readAt := 0
	// fmt.Printf("Reading %s for %d bytes at position %d...\n", localPath, nToRead, readAt)
	// time.Sleep(2 * time.Second)

	// expected, err := c1.ReadAt(fd, readAt, nToRead)
	// if err != nil {
	// 	fmt.Printf("read testidempotent.txt error: %v\n", err)
	// 	return
	// }
	// fmt.Printf("Expected read file content: \n%s\n", string(expected))

	// time.Sleep(4 * time.Second)

	actual, err := c1.Read(fd, nToRead)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("Actual read file content: \n%s\n", string(actual))
}

func AtLeastOnceIdempotentRead() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 50
	rpc.Timeout = 100 * time.Millisecond
	performIdempotentRead()
}

func AtLeastOnceNonIdempotentRead() {
	// rpc.FilterDuplicatedRequest = false
	// rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 50
	rpc.Timeout = 100 * time.Millisecond
	performNonIdempotentRead()
}

func AtMostOnceIdempotentRead() {
	// rpc.FilterDuplicatedRequest = true
	// rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 50
	rpc.Timeout = 100 * time.Millisecond
	performIdempotentRead()
}

func AtMostOnceNonIdempotentRead() {
	// rpc.FilterDuplicatedRequest = true
	// rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 50
	rpc.Timeout = 100 * time.Millisecond
	performNonIdempotentRead()
}

func main() {
	SimpleTest()
}
