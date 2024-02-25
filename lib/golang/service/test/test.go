package main

import (
	"log"
	"distributed-file-system/lib/golang/service"
	"distributed-file-system/lib/golang/config"
)

func main() {
	conf := config.GetConfig()
	log.Printf("config: %v", conf)
	server := service.NewFileServer(config.GetConfig())
	go server.Run()

	client := service.NewFileClient()
	client.Mount("127.0.0.1:8080", "/Users/maffinnn/Dev/distributed-file-system/mockdir/mockfile.txt", "test.txt", "")

}