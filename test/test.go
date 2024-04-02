package main

import (
	"fmt"
	"time"

	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
)

var serverAddr string = ":8080"
var timeFormat string = "2006-01-02 15:04:05"

func TestCreateService() {
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.SunNetworkFileSystemType)

	fd, err := c1.Create("1/subdir1/testcreate1.txt")
	if err != nil {
		fmt.Printf("create error %v", err)
		return
	}
	c1.ListAllFiles()

	n, err := c1.Write(fd, 0, []byte("test create file\n"))
	if err != nil {
		fmt.Printf("write error %v", err)
		return
	}
	fmt.Printf("%d bytes written to %s\n", n, "1/subdir1/testcreate1.txt")
	data, err := c1.ReadAt(fd, 0, 1000)
	if err != nil {
		fmt.Printf("read error %v", err)
		return
	}
	fmt.Printf("\nReading 1/subdir1/testcreate1.txt...\n\n%s\n\n", string(data))

	c1.Close(fd)
}

func senario1(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fdC1, err := c1.Open("1/testfile3.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}

	data, err := c1.ReadAt(fdC1, 0, 1000) // read all the content
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 1/testfile3.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)

	// client 2 opens the file
	fdC2, err = c2.Open("2/testfile3.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	// client 2 updates the file
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 2/testfile3.txt...\n\n%s\n\n", string(data))
	n, err := c2.Write(fdC2, 0, []byte(fmt.Sprintf("write to file at %s by client 2\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testfile3.txt")
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 2/testfile3.txt...\n\n%s\n\n", string(data))
	c2.Close(fdC2)

	time.Sleep(2 * time.Second)
	// client 1 reads the file
	fdC1, err = c1.Open("1/testfile3.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 1/testfile3.txt...\n\n%s\n\n", string(data))

	// client 2 updates the file again
	n, err = c2.Write(fdC2, 0, []byte(fmt.Sprintf("write to file at %s by client 2 again\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testfile3.txt")
	c2.Close(fdC2)

	time.Sleep(2 * time.Second)

	// client 1 reads the file
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 1/testfile3.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)
}

func senario2(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fdC1, err := c1.Open("1/testfile2.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	// client 2 opens the file
	fdC2, err = c2.Open("2/testfile2.txt")
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	// client 1 reads
	data, err := c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 1/testfile2.txt...\n\n%s\n\n", string(data))

	// client 2 updates the file and close
	n, err := c2.Write(fdC2, 0, []byte(fmt.Sprintf("write to file at %s by client 2\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testfile2.txt")
	c2.Close(fdC2)

	time.Sleep(2 * time.Second)

	// client 1 updates the file
	n, err = c1.Write(fdC1, 0, []byte(fmt.Sprintf("write to file at %s by client 1\n", time.Now().Format(timeFormat))))
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "1/testfile2.txt")
	time.Sleep(2 * time.Second)
	// client 1 reads the file
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		fmt.Printf("%+v", err)
		return
	}
	fmt.Printf("\nReading 1/testfile2.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)
}

func TestCacheConsistencyAFSSenario1() {
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.AndrewFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.AndrewFileSystemType)
	senario1(c1, c2)
}

func TestCacheConsistencyAFSSenario2() {
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.AndrewFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.AndrewFileSystemType)
	senario2(c1, c2)
}

func TestCacheConsistencyNFSSenario1() {
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	service.PollInterval = 100 // in milliseconds

	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(1 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.SunNetworkFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.SunNetworkFileSystemType)
	senario1(c1, c2)
}

func TestCacheConsistencyNFSSenario2() {
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond
	service.PollInterval = 100 // in milliseconds

	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(2 * time.Second)

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	c2 := service.NewFileClient("2", ":8082", serverAddr)
	go c1.Run()
	go c2.Run()

	c1.Mount("etc/exports/mockdir1", "1", service.SunNetworkFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.SunNetworkFileSystemType)
	senario2(c1, c2)
}

func performIdempotentRead() {
	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(2 * time.Second) // to make sure server is up

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	time.Sleep(2 * time.Second)
	err := c1.Mount("etc/exports/mockdir1/subdir3/testidempotent.txt", "localfile/1/testidempotent.txt", service.SunNetworkFileSystemType)
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
	data, err := c1.ReadAt(fd, 0, 50)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("Read file content:\n%s\n", string(data))
}

func performNonIdempotentRead() {
	// init server
	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(2 * time.Second) // to make sure server is up

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	time.Sleep(2 * time.Second)
	err := c1.Mount("etc/exports/mockdir1/subdir3/testidempotent.txt", "localfile/1/testidempotent.txt", service.SunNetworkFileSystemType)
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

	// time.Sleep(2 * time.Second)
	// expected, err := c1.ReadAt(fd, 0, 50)
	// if err != nil {
	// 	fmt.Printf("read testidempotent.txt error: %v\n", err)
	// 	return
	// }
	// fmt.Printf("\033[33;1mExpected read file content: %s\n\033[0m", string(expected))

	time.Sleep(3 * time.Second)
	actual, err := c1.Read(fd, 50)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("\033[33;1mActual read file content: %s\n\033[0m", string(actual))
}

func AtLeastOnceIdempotentRead() {
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 50
	rpc.Timeout = 100 * time.Millisecond
	performIdempotentRead()
}

func AtLeastOnceNonIdempotentRead() {
	// set up
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 750 * time.Millisecond
	performNonIdempotentRead()
}

func AtMostOnceIdempotentRead() {
	rpc.FilterDuplicatedRequest = true
	rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 50
	rpc.Timeout = 200 * time.Millisecond
	performIdempotentRead()
}

func AtMostOnceNonIdempotentRead() {
	rpc.FilterDuplicatedRequest = true
	rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 800 * time.Millisecond
	performNonIdempotentRead()
}

func main() {
	TestCacheConsistencyNFSSenario2()
}
