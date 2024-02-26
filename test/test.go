package main

import (
	"bufio"
	"log"
	"os"

	"distributed-file-system/lib/golang/config"
	"distributed-file-system/lib/golang/service"
	"distributed-file-system/lib/golang/service/file"
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
	server := file.NewFileServer(config.GetConfig())
	go server.Run()

	client := client.NewFileClient("1", conf)
	client.Mount("distributed-file-system/mockdir1", "testmount", "")
	client.Read("testmount/subdir1/pg-being_ernest.txt", 0, 250)
	client.Create("testmount/testcreate1.txt")
	client.Write("testmount/testcreate.txt", 0, []byte("insert at front on monday"))
	client.Remove("testmount/testcreate.txt")
}
