package rpc

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	RetryLimit                   uint64        = 7
	Timeout                      time.Duration = 10 * time.Millisecond
	NetworkPacketLossProbability float64       = 0.7
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Call represents an active RPC.
type Call struct {
	Attempts         atomic.Uint64 // count number of attempts made
	LastTryTimestamp time.Time     // timestamp for last retry attempt
	Seq              uint64
	ServiceMethod    string      // format "<service>.<method>"
	Args             interface{} // arguments to the function
	Reply            interface{} // reply from the function
	Error            error       // if error occurs, it will be set
	Done             chan *Call  // Strobes when call is complete.
}

func (call *Call) done() {
	call.Done <- call
}

// Client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type Client struct {
	conn     *net.UDPConn
	cc       Codec
	sending  sync.Mutex // protect following
	mu       sync.Mutex // protect following
	seq      uint64
	pending  sync.Map
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// Close the connection
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.conn.Close()
}

// IsAvailable return true if the client does work
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending.Store(call.Seq, call)
	client.seq++
	return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	v, _ := client.pending.LoadAndDelete(seq)
	if v == nil {
		return nil
	}
	return v.(*Call)
}

func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	client.pending.Range(func(key, value interface{}) bool {
		call := value.(*Call)
		call.Error = err
		call.done()
		return true
	})
}

func (client *Client) retry() {
	RetryLimit = math.MaxUint64 // retry until success
	for {
		client.pending.Range(func(key, value interface{}) bool {
			call := value.(*Call)
			if call.Attempts.Load() >= RetryLimit {
				client.terminateCalls(fmt.Errorf("rpc client packet %d lost due to poor internet connection", call.Seq))
				return true
			}
			if time.Since(call.LastTryTimestamp) >= Timeout {
				// try again
				client.send(call.Seq, call)
			}
			return true
		})
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		buf := make([]byte, MaxBufferSize)
		n, _, err := client.conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("rpc client: error reading from UDP: %v", err)
		}
		var m Message
		err = client.cc.Decode(buf[:n], &m)
		if err != nil {
			log.Printf("rpc client: error decode the message: %v", err)
		}
		log.Printf("rpc client response for packet seq %d is received.\n", m.Header.Seq)
		h := m.Header
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// it usually means that Write partially failed
			// and call was already removed.
		case h.Error != "":
			call.Error = errors.New(h.Error)
			call.done()
		default:
			deepCopy(m.Body, call.Reply)
			call.done()
		}
	}
	// error occurs, so terminateCalls pending calls
	client.terminateCalls(err)
}

func NewClient(conn *net.UDPConn) *Client {
	codecFunc := NewCodecFuncMap[DefaultCodecType]
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		conn:    conn,
		cc:      codecFunc(),
		pending: sync.Map{},
	}
	go client.receive()
	go client.retry()
	return client
}

// Dial connects to an RPC server at the specified network address
func Dial(addr string) (*Client, error) {
	s, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal("error resolving udp address: ", err)
	}
	conn, err := net.DialUDP("udp", nil, s)
	if err != nil {
		return nil, err
	}
	client := NewClient(conn)
	// close the connection if client is nil
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return client, nil
}

func (client *Client) send(seq uint64, call *Call) {
	// make sure that the client will send a complete request
	client.sending.Lock()
	defer client.sending.Unlock()

	// prepare request header
	var header Header
	header.ServiceMethod = call.ServiceMethod
	header.Seq = seq

	// encode and send the request
	data, err := client.cc.Encode(&header, call.Args)
	if err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
		return
	}
	call.Attempts.Add(1)
	// simulate packet loss
	// rand.Float64 generates a float64 f: 0.0 <= f < 1.0
	if rand.Float64() < NetworkPacketLossProbability {
		log.Printf("rpc client: packet seq %d is sent but lost.", header.Seq)
		return
	}
	// log.Printf("sending packet %d...", header.Seq)
	_, err = client.conn.Write(data)
	if err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
		return
	}
}

// Go invokes the function asynchronously.
// It returns the Call structure representing the invocation.
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod:    serviceMethod,
		Args:             args,
		Reply:            reply,
		LastTryTimestamp: time.Now(),
		Done:             done,
	}

	// register this call.
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return call
	}

	client.send(seq, call)
	return call
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
