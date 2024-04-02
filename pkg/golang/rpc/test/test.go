package main

import (
	"distributed-file-system/pkg/golang/rpc"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const PORT string = ":8080"

type Foo int

type Bar string

type Args struct{ Num1, Num2 int64 }
type Reply struct{ Num int64 }

func (f Foo) Sum(args Args, reply *Reply) error {
	reply.Num = args.Num1 + args.Num2
	return nil
}

func (f Foo) Mount(args Args, reply *Reply) error {
	fmt.Printf("mount is called\n")
	return nil
}

func startServer() {
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

func TestRPC() {
	go startServer()
	client, _ := rpc.Dial(PORT)
	time.Sleep(time.Second)
	// send request & receive response
	rpc.RegisterType(Args{})
	rpc.RegisterType(Reply{})
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: int64(i), Num2: int64(i * i)}
			var reply Reply
			if err := client.Call("Foo.Sum", args, &reply); err != nil {
				fmt.Println("call Foo.Sum error:", err)
				return
			}
			fmt.Printf("client: %d + %d = %d\n", args.Num1, args.Num2, reply.Num)
		}(i)
	}
	wg.Wait()
}

func TestRPCSingle() {
	go startServer()
	client, _ := rpc.Dial(PORT)
	time.Sleep(time.Second)
	// send request & receive response
	wait := make(chan struct{})
	rpc.RegisterType(Args{})
	rpc.RegisterType(Reply{})
	i := 2
	args := &Args{Num1: int64(i), Num2: int64(i * i)}
	var reply int64
	if err := client.Call("Foo.Sum", args, &reply); err != nil {
		fmt.Println("call Foo.Sum error:", err)
		return
	}
	fmt.Printf("client: %d + %d = %d\n", args.Num1, args.Num2, reply)
	<-wait
}

type request struct {
	ClientId   string
	ClientAddr string
	FilePath   string
	Data       []byte
	Timestamp  int64
	Success    bool
}

func main() {
	var h rpc.Header
	h.ServiceMethod = "Foo.Sum"
	h.Seq = 43
	// var body int = 1
	// fmt.Printf("%#v\n", h.ServiceMethod)
	// registeredMethods := []string{"FileServer.Mount", "FileServer.Unmount", "FileServer.GetAttribute", "FileServer.Create", "FileServer.Read", "FileServer.Write", "FileServer.Remove"}
	// for _, m := range registeredMethods {
	// 	fmt.Println(unsafe.Sizeof(m))
	// }

	var m rpc.Message
	m.Header = h
	// m.Body = &Args{Num1: 3, Num2: 10}
	m.Body = request{
		ClientId:   "",
		ClientAddr: "255.255.255.255:8080",
		FilePath:   "",
		// Data:       []byte("Hello test encoding and decoding"),
		Timestamp: 13213132940,
		Success:   false,
	}
	// fmt.Printf("size of header: %d\n", unsafe.Sizeof(m.Header))
	// fmt.Printf("size of service method: %d\n", unsafe.Sizeof(m.Header.ServiceMethod))
	// fmt.Printf("size of seq: %d\n", unsafe.Sizeof(m.Header.Seq))
	// fmt.Printf("size of err code: %d\n", unsafe.Sizeof(m.Header.ErrCode))
	// p := unsafe.Pointer(& m)
	// s := *(*[16]byte)(p)
	// fmt.Printf("first 16 bytes: %s\n", string(s[:]))

	// u := (*interface{})(unsafe.Add(p, unsafe.Offsetof(m.Body)))

	// fmt.Printf("body: %+v\n", (*u).(request))

	codec := rpc.NewLabCodec()
	// b, _ := corba.EncodeHeader(&h)
	// fmt.Printf("byte stream: %+v\n", b)

	// var newHeader rpc.Header
	// corba.DecodeHeader(b, &newHeader)
	// fmt.Printf("%+v\n", newHeader)
	// fmt.Printf("service method: %s\n", string(newHeader.ServiceMethod[:]))
	// corba := codec.NewCorbaCodec()
	// b, err := corba.Encode(&h, body)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(b)
	rpc.RegisterType(request{})

	b, _ := codec.Encode(&m.Header, m.Body)
	// fmt.Printf("byte stream in body: %+v\n", b)
	var newM rpc.Message
	err := codec.Decode(b, &newM)
	if err != nil {
		log.Fatalf("decode error: %v", err)
	}
	fmt.Printf("decoded header: %+v\n", newM.Header)
	fmt.Printf("decoded object: %+v\n", newM.Body)

	TestRPC()
}
