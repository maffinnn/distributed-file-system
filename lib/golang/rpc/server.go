package rpc

import (
	"errors"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
)

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(conn *net.UDPConn) { DefaultServer.Accept(conn) }

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// Server represents an RPC Server.
type Server struct {
	cc         Codec
	serviceMap sync.Map
	close      chan struct{}
}

// NewServer returns a new Server.
func NewServer() *Server {
	codecFunc := NewCodecFuncMap[DefaultCodecType]
	return &Server{
		cc:    codecFunc(),
		close: make(chan struct{}),
	}
}

// DefaultServer is the default instance of *Server.
var DefaultServer = NewServer()

// Register publishes in the server the set of methods of the
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func (server *Server) Accept(conn *net.UDPConn) {
	for {
		select {
		case <-server.close:
			log.Printf("rpc server: closing connection...")
			return
		default:
			buf := make([]byte, MaxBufferSize)
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				log.Println("rpc server: read udp error:", err)
				return
			}

			// fmt.Printf("server reads from udp socket %d bytes\n", n)
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

func (server *Server) readRequest(data []byte) (*request, error) {
	var m Message
	err := server.cc.Decode(data, &m)
	if err != nil {
		log.Printf("server readRequest: decode error: %v", err)
		return nil, err
	}
	// log.Print("server readRequest: %+v", m)
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
	server.sendResponse(conn, addr, req.h, req.replyv.Interface())
}

func (server *Server) sendResponse(conn *net.UDPConn, addr *net.UDPAddr, h *Header, body interface{}) {
	data, err := server.cc.Encode(h, body)
	if err != nil {
		log.Println("rpc server: encode response error:", err)
		return
	}

	// simulate packet loss
	// rand.Float64 generates a float64 f: 0.0 <= f < 1.0
	// if rand.Float64() < NetworkPacketLossProbability {
	// 	return
	// }

	_, err = conn.WriteToUDP(data, addr)
	if err != nil {
		log.Println("rpc server: write response error:", err)
		return
	}
}
