package main

import (
	"bufio"
	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/service"
	"log"
	"os"
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

	client := service.NewFileClient(conf)
	client.Mount("distributed-file-system/mockdir", "testmount", "")
	client.Read("distributed-file-system/mockdir", "pg-being_ernest.txt", 0, 250)
	client.Create("src/distributed-file-system/mockdir", "testcreate.txt")
	client.Write("src/distributed-file-system/mockdir", "testcreate.txt", 0, []byte("insert at front"))
	//client.Write("src/distributed-file-system/mockdir", "testcreate.txt", 1, []byte("inserting file..."))
	//client.Remove("src/distributed-file-system/mockdir", "testcreate.txt")
}
