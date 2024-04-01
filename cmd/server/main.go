package main

import (
	"distributed-file-system/pkg/golang/service"
	"flag"
)

func main() {
	addr := flag.String("addr", ":8080", "server address")
	flag.Parse()
	server := service.NewFileServer(*addr)
	server.Run()
}
