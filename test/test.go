package main

import (
	"fmt"
	"log"
	"time"

	"distributed-file-system/pkg/golang/rpc"
	"distributed-file-system/pkg/golang/service"
)

var serverAddr string = ":8080"

func senario1(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fdC1, err := c1.Open("1/testcreate3.txt")
	if err != nil {
		log.Println(err)
	}
	data, err := c1.ReadAt(fdC1, 0, 1000) // read all the content
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate3.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)

	// client 2 opens the file
	fdC2, err = c2.Open("2/testcreate3.txt")
	if err != nil {
		log.Fatal(err)
	}
	// client 2 updates the file
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 2/testcreate3.txt...\n\n%s\n\n", string(data))
	n, err := c2.Write(fdC2, 0, []byte("insert on monday by c2\n"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testcreate3.txt")
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 2/testcreate3.txt...\n\n%s\n\n", string(data))

	time.Sleep(2 * time.Second)
	// client 1 reads the file
	fdC1, err = c1.Open("1/testcreate3.txt")
	if err != nil {
		log.Println(err)
	}
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate3.txt...\n\n%s\n\n", string(data))

	// client 2 updates the file again
	n, err = c2.Write(fdC2, 0, []byte("insert on monday by c2 again\n"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testcreate3.txt")
	c2.Close(fdC2)

	time.Sleep(2 * time.Second)

	// client 1 reads the file
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate3.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)
}

func senario2(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fdC1, err := c1.Open("1/testcreate2.txt")
	if err != nil {
		log.Println(err)
	}
	// client 2 opens the file
	fdC2, err = c2.Open("2/testcreate2.txt")
	if err != nil {
		log.Println(err)
	}
	// client 1 reads
	data, err := c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate2.txt...\n\n%s\n\n", string(data))

	// client 2 updates the file and close
	n, err := c2.Write(fdC2, 0, []byte("insert on monday by c2\n"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testcreate2.txt")
	c2.Close(fdC2)

	time.Sleep(2 * time.Second)
	// client 1 updates the file
	n, err = c1.Write(fdC1, 0, []byte("insert on monday by c1\n"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "1/testcreate2.txt")
	time.Sleep(2 * time.Second)
	// client 1 reads the file
	data, err = c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate2.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)
}

func TestCacheConsistencyAFSSenario1() {
	rpc.LogInfo = false
	rpc.FilterDuplicatedRequest = true
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
	rpc.LogInfo = false
	rpc.FilterDuplicatedRequest = true
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
	rpc.LogInfo = false
	rpc.FilterDuplicatedRequest = true
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond

	service.PollInterval = 10 // in milliseconds

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
	rpc.LogInfo = false
	rpc.FilterDuplicatedRequest = true
	rpc.ServerSideNetworkPacketLossProbability = 0
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 100 * time.Millisecond
	service.PollInterval = 10 // in milliseconds

	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(1 * time.Second)

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

	time.Sleep(1 * time.Second) // to make sure server is up

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	err := c1.Mount("etc/exports/mockdir1/subdir3/testidempotent.txt", "localfile/1", service.SunNetworkFileSystemType)
	if err != nil {
		fmt.Printf("mount error: %v\n", err)
		return
	}

	localPath := "localfile/1/testidempotent.txt"
	fd, err := c1.Open(localPath)
	if err != nil {
		fmt.Printf("open testidempotent.txt error: %v\n", err)
		return
	}
	data, err := c1.ReadAt(fd, 0, 10)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("read file content:\n%s\n", string(data))
}

func performNonIdempotentRead() {
	// init server
	server := service.NewFileServer(serverAddr)
	go server.Run()

	time.Sleep(1 * time.Second) // to make sure server is up

	c1 := service.NewFileClient("1", ":8081", serverAddr)
	go c1.Run()

	err := c1.Mount("etc/exports/mockdir1/subdir3/testidempotent.txt", "localfile/1", service.SunNetworkFileSystemType)
	if err != nil {
		fmt.Printf("mount error: %v\n", err)
		return
	}

	localPath := "localfile/1/testidempotent.txt"
	fd, err := c1.Open(localPath)
	if err != nil {
		fmt.Printf("open testidempotent.txt error: %v\n", err)
		return
	}

	expected, err := c1.ReadAt(fd, 0, 10)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("Expected read file content: %s\n", string(expected))

	actual, err := c1.Read(fd, 10)
	if err != nil {
		fmt.Printf("read testidempotent.txt error: %v\n", err)
		return
	}
	fmt.Printf("Actual read file content: %s\n", string(actual))
}

func AtLeastOnceIdempotentRead() {
	rpc.LogInfo = true
	rpc.FilterDuplicatedRequest = false
	rpc.ServerSideNetworkPacketLossProbability = 20
	rpc.ClientSideNetworkPacketLossProbability = 20
	rpc.Timeout = 100 * time.Millisecond
	performIdempotentRead()
}

func AtLeastOnceNonIdempotentRead() {
	// set up
	rpc.FilterDuplicatedRequest = false
	rpc.LogInfo = false
	rpc.ServerSideNetworkPacketLossProbability = 60
	rpc.ClientSideNetworkPacketLossProbability = 10
	rpc.Timeout = 750 * time.Millisecond
	performNonIdempotentRead()
}

func AtMostOnceIdempotentRead() {
	rpc.LogInfo = true
	rpc.FilterDuplicatedRequest = true
	rpc.ServerSideNetworkPacketLossProbability = 10
	rpc.ClientSideNetworkPacketLossProbability = 10
	rpc.Timeout = 200 * time.Millisecond
	performIdempotentRead()
}

func AtMostOnceNonIdempotentRead() {
	rpc.FilterDuplicatedRequest = true
	rpc.LogInfo = false
	rpc.ServerSideNetworkPacketLossProbability = 50
	rpc.ClientSideNetworkPacketLossProbability = 0
	rpc.Timeout = 750 * time.Millisecond
	performNonIdempotentRead()
}
func main() {
	TestCacheConsistencyAFSSenario2()
}
