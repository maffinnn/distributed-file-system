package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/service"
)

func run() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "q" {
			log.Printf("Exiting the program")
			break
		}
		log.Println(line)
	}
}

func Senario1(c1, c2 *service.FileClient) {
	var fdC1, fdC2 *service.FileDescriptor
	// client 1 opens the file
	fdC1, err := c1.Open("1/testcreate3.txt")
	if err != nil {
		log.Println(err)
	}
	data, err := c1.ReadAt(fdC1, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate3.txt...\n\n%s\n\n", string(data))
	c1.Close(fdC1)

	// client 2 opens the file
	fdC2, err = c2.Open("2/testcreate3.txt")
	if err != nil {
		log.Println(err)
	}
	// client 2 updates the file
	data, err = c2.ReadAt(fdC2, 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 2/testcreate3.txt...\n\n%s\n\n", string(data))
	n, err := c2.Write(fdC2, 0, []byte("insert on friday by c2\n"))
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

	// client 2 updates the file once again
	n, err = c2.Write(fdC2, 0, []byte("insert on friday by c2 again\n"))
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

func Senario2(c1, c2 *service.FileClient) {
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
	n, err := c2.Write(fdC2, 0, []byte("insert on friday by c2\n"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testcreate2.txt")
	c2.Close(fdC2)

	time.Sleep(2 * time.Second)
	// client 1 updates the file
	n, err = c1.Write(fdC1, 0, []byte("insert on friday by c1\n"))
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

func TestCacheConsistencyAFSSenario1(conf *config.Config) {
	c1 := service.NewFileClient("1", ":8081", conf)
	c2 := service.NewFileClient("2", ":8082", conf)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.AndrewFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.AndrewFileSystemType)
	Senario1(c1, c2)
}

func TestCacheConsistencyAFSSenario2(conf *config.Config) {
	c1 := service.NewFileClient("1", ":8081", conf)
	c2 := service.NewFileClient("2", ":8082", conf)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.AndrewFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.AndrewFileSystemType)
	Senario2(c1, c2)
}

func TestCacheConsistencyNFSSenario1(conf *config.Config) {
	c1 := service.NewFileClient("1", ":8081", conf)
	c2 := service.NewFileClient("2", ":8082", conf)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.SunNetworkFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.SunNetworkFileSystemType)
	Senario1(c1, c2)
}

func TestCacheConsistencyNFSSenario2(conf *config.Config) {
	c1 := service.NewFileClient("1", ":8081", conf)
	c2 := service.NewFileClient("2", ":8082", conf)
	go c1.Run()
	go c2.Run()
	c1.Mount("etc/exports/mockdir1", "1", service.SunNetworkFileSystemType)
	c2.Mount("etc/exports/mockdir1", "2", service.SunNetworkFileSystemType)
	Senario2(c1, c2)
}

func main() {
	pwd, _ := os.Getwd()
	log.Println(pwd)
	conf := config.GetConfig()
	log.Println(conf)
	server := service.NewFileServer(config.GetConfig())
	go server.Run()
	TestCacheConsistencyAFSSenario2(conf)
}
