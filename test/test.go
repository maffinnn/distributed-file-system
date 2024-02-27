package main

import (
	"bufio"
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
	client := service.NewFileClient("1", conf)
	go client.Run()
	client.Mount("distributed-file-system/mockdir1", "testmount", 10)
	client.Read("testmount/subdir1/pg-being_ernest.txt", 0, 250)
	client.Create("testmount/testcreate3.txt")
	client.Create("testmount/testcreate1.txt")
	client.Write("testmount/testcreate3.txt", 0, []byte("insert at front on monday"))
	client.Remove("testmount/testcreate1.txt")
	client.MakeDir("testmount/subdir2/subdir1")
	client.MakeDir("testmount/subdir2/subdir2")
	client.RemoveDir("testmount/subdir2/subdir1")
}
