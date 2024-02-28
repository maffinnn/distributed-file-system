package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

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

func main() {
	pwd, _ := os.Getwd()
	log.Println(pwd)
	conf := config.GetConfig()
	log.Println(conf)
	server := service.NewFileServer(config.GetConfig())
	go server.Run()
	c1 := service.NewFileClient("1", ":8081", conf)
	c2 := service.NewFileClient("2", ":8082", conf)
	go c1.Run()
	go c2.Run()
	wait := make(chan struct{})
	c1.Mount("distributed-file-system/mockdir1", "1", 10)
	c2.Mount("distributed-file-system/mockdir1", "2", 20)
	// data, err := c1.ReadAt("1/subdir1/pg-being_ernest.txt", 0, 100)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("\nReading 1/subdir1/pg-being_ernest.txt...\n\n%s\n\n", string(data))
	c1.Create("1/testcreate3.txt")
	// c1.Create("1/testcreate1.txt")
	n, err := c1.Write("1/testcreate3.txt", 0, []byte("insert on tuesday by c1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "1/testcreate3.txt")
	data, err := c1.ReadAt("1/testcreate3.txt", 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate3.txt...\n\n%s\n\n", string(data))
	data, err = c2.ReadAt("2/testcreate3.txt", 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 2/testcreate3.txt...\n\n%s\n\n", string(data))
	n, err = c2.Write("2/testcreate3.txt", 0, []byte("insert on tuesday by c2"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n%d bytes are written to %s\n", n, "2/testcreate3.txt")
	data, err = c1.ReadAt("1/testcreate3.txt", 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nReading 1/testcreate3.txt...\n\n%s\n\n", string(data))
	// c1.Remove("1/testcreate1.txt")
	// c1.MakeDir("1/subdir2/subdir1")
	// c1.MakeDir("1/subdir2/subdir2")
	// c1.RemoveDir("1/subdir2/subdir1")
	<-wait
}
