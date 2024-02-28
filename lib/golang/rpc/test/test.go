package main

import (
	"log"
	"sync"
	"time"
	"net"
	"fmt"
	"distributed-file-system/lib/golang/rpc"
)

const PORT string = ":8080"

type Foo int

type Bar string

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (b Bar) Print(args Args, reply *string) error {
	log.Printf("%v", args)
	*reply = fmt.Sprintf("%d + %d", args.Num1, args.Num2)
	return nil
}

func startServer(){
	var foo Foo
	if err := rpc.Register(&foo); err != nil {
		log.Fatal("register error:", err)
	}
	s, err := net.ResolveUDPAddr("udp", PORT)
	if err != nil {
		log.Fatal("client error resolving udp address: ", err)
	}
	conn, err := net.ListenUDP("udp", s)
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", conn.LocalAddr().String())
	rpc.Accept(conn)
}

func main() {
	go startServer()
	client, _ := rpc.Dial(PORT)
	time.Sleep(time.Second)
	// send request & receive response
	rpc.RegisterType(Args{})
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			if err := client.Call("Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Printf("client: %d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}