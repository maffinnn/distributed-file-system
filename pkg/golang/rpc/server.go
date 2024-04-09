package rpc

import (
	"distributed-file-system/pkg/golang/logger"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

// default setting
var (
	FilterDuplicatedRequest                bool          = true
	CacheValidityPeriod                    time.Duration = 3 * time.Minute
	ServerSideNetworkPacketLossProbability int           = 50
	randomNumberGenerator                                = rand.New(rand.NewSource(50))
)

// Server represents an RPC Server.
type Server struct {
	receiving  sync.Mutex // guard for receiving
	sending    sync.Mutex // guard for sending
	mu         sync.Mutex
	cc         Codec
	serviceMap sync.Map // to store the registered service
	processed  sync.Map // processed message store
	close      chan struct{}
	logger     *logger.Logger
}

// NewServer returns a new Server.
func NewServer(logger *logger.Logger) *Server {
	codecFunc := NewCodecFuncMap[DefaultCodecType]
	s := &Server{
		cc:     codecFunc(),
		close:  make(chan struct{}),
		logger: logger,
	}
	go s.backgroundCleanUp()
	return s
}

// Register publishes in the server the set of methods of the
func (server *Server) Register(rcvr interface{}) error {
	s, err := newService(rcvr)
	if err != nil {
		return err
	}
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return fmt.Errorf("rpc: service already defined: %s ", s.name)
	}
	return nil
}

func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = fmt.Errorf("rpc server: service/method request ill-formed: %s", serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = fmt.Errorf("rpc server: can't find service %s", serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = fmt.Errorf("rpc server: can't find method %s", methodName)
	}
	return
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func (server *Server) Accept(conn *net.UDPConn) {
	for {
		select {
		case <-server.close:
			server.logger.Printf("[INFO] rpc server: closing connection...")
			return
		default:
			buf := make([]byte, MaxBufferSize)
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				server.logger.Printf("[ERROR] rpc server: read udp error: %V", err)
				return
			}
			go server.ServeConn(conn, addr, buf[:n])
		}
	}
}

// ServeConn runs the server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
func (server *Server) ServeConn(conn *net.UDPConn, addr *net.UDPAddr, data []byte) {

	req, err := server.readRequest(data)
	if err != nil {
		if req == nil {
			return // it's not possible to recover, so close the connection
		}
		req.h.Error = err.Error()
		server.sendResponse(conn, addr, req.h, invalidRequest)
		return
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	// log.Printf("rpc server: packet seq %d from %s has been received\n", req.h.Seq)
	// check for request duplication
	if FilterDuplicatedRequest {
		id := fmt.Sprintf("%s-%d", addr.String(), req.h.Seq)
		v, ok := server.processed.Load(id)
		if ok {
			// exists
			server.logger.Printf("[INFO] rpc server: duplicated request %s, sending from cached result.\n", id)
			c := v.(*cachedResponse)
			server.sendResponse(conn, addr, req.h, c.replyv.Interface())
			return
		}
	}
	server.handleRequest(conn, addr, req)
}

func (server *Server) Shutdown() {
	server.close <- struct{}{}
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// request stores all information of a call
type request struct {
	h            *Header       // header of request
	argv, replyv reflect.Value // argv and replyv of request
	mtype        *methodType   // type of request
	svc          *service
}

type cachedResponse struct {
	timestamp time.Time     // timestamp
	replyv    reflect.Value // replyv
}

func (server *Server) readRequest(data []byte) (*request, error) {
	server.receiving.Lock()
	defer server.receiving.Unlock()
	var m Message
	err := server.cc.Decode(data, &m)
	if err != nil {
		server.logger.Printf("[] server readRequest: decode error: %v", err)
		return nil, err
	}

	req := &request{h: &m.Header}
	req.svc, req.mtype, err = server.findService(m.Header.ServiceMethod)
	if err != nil {
		return req, err
	}

	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()
	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	deepCopy(m.Body, argvi)
	return req, nil
}

func (server *Server) handleRequest(conn *net.UDPConn, addr *net.UDPAddr, req *request) {
	err := req.svc.call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.h.Error = err.Error()
		server.sendResponse(conn, addr, req.h, invalidRequest)
		return
	}
	// store the request result
	id := fmt.Sprintf("%s-%d", addr.String(), req.h.Seq)
	server.processed.Store(id, &cachedResponse{time.Now(), req.replyv})
	server.sendResponse(conn, addr, req.h, req.replyv.Interface())
}

func (server *Server) sendResponse(conn *net.UDPConn, addr *net.UDPAddr, h *Header, body interface{}) {
	server.sending.Lock()
	defer server.sending.Unlock()
	data, err := server.cc.Encode(h, body)
	if err != nil {
		server.logger.Printf("[ERROR] rpc server: encode response error: %v", err)
		return
	}

	// simulate packet loss
	if randomNumberGenerator.Intn(100) < ServerSideNetworkPacketLossProbability {
		server.logger.Printf("[INFO] rpc server: packet %s is sent but lost.", fmt.Sprintf("%s-%d", addr.String(), h.Seq))
		return
	}

	_, err = conn.WriteToUDP(data, addr)
	if err != nil {
		server.logger.Printf("[ERROR] rpc server: write response error: %v", err)
		return
	}
}

func (server *Server) backgroundCleanUp() {
	for {
		select {
		case <-server.close:
			return
		default:
			server.processed.Range(func(key, value interface{}) bool {
				c := value.(*cachedResponse)
				if time.Since(c.timestamp) > CacheValidityPeriod {
					server.processed.LoadAndDelete(key)
				}
				return true
			})
		}
	}
}
