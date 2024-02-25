package main

import (
	"bufio"
	"os"
	"log"
	"distributed-file-system/lib/golang/service"
	"distributed-file-system/lib/golang/config"
)

func run(){
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
	conf := config.GetConfig()
	log.Printf("config: %v", conf)
	server := service.NewFileServer(config.GetConfig())
	go server.Run()

	client := service.NewFileClient(conf)
	client.Mount("/Users/maffinnn/Dev/distributed-file-system/mockdir", "testmount", "")
	client.Read("/Users/maffinnn/Dev/distributed-file-system/mockdir", "pg-being_ernest.txt", 0, 250)
	client.Create("/Users/maffinnn/Dev/distributed-file-system/mockdir", "testcreate.txt")
}