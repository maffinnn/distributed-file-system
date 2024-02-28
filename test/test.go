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
	client := service.NewFileClient("1", ":8081", conf)
	go client.Run()
	wait := make(chan struct{})
	client.Mount("distributed-file-system/mockdir1", "testmount", 0)
	data, err := client.Read("testmount/subdir1/pg-being_ernest.txt", 0, 100)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Reading testmount/subdir1/pg-being_ernest.txt...\n%s\n", string(data))
	client.Create("testmount/testcreate3.txt")
	client.Create("testmount/testcreate1.txt")
	n, err := client.Write("testmount/testcreate3.txt", 3, []byte("insert at front on tuesday"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d bytes are written to %s", n, "testmount/testcreate3.txt")
	data, err = client.Read("testmount/testcreate3.txt", 0, 1000)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Reading testmount/testcreate3.txt...\n%s\n", string(data))
	client.Remove("testmount/testcreate1.txt")
	client.MakeDir("testmount/subdir2/subdir1")
	client.MakeDir("testmount/subdir2/subdir2")
	client.RemoveDir("testmount/subdir2/subdir1")
	<-wait
}
