package rpc

import (
	"log"
	"net"
	"reflect"
	"sync"
	"errors"
	"strings"

	"distributed-file-system/lib/golang/rpc/codec"
)

const defaultCodecType = codec.GobType

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(conn *net.UDPConn) { DefaultServer.Accept(conn) }

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// Server represents an RPC Server.
type Server struct{
	cc codec.Codec
	serviceMap sync.Map
}

// NewServer returns a new Server.
func NewServer() *Server {
	codecFunc := codec.NewCodecFuncMap[defaultCodecType]
	return &Server{
		cc: codecFunc(),
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
		buf := make([]byte, 1024)
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("rpc server: read udp error:", err)
			return
		}
		go server.ServeConn(conn, addr, buf[:n])
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

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}


// request stores all information of a call
type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and replyv of request
	mtype 		*methodType // type of request
	svc 		*service
}

func (server *Server) readRequest(data []byte) (*request, error) {
	var m codec.Message
	err := server.cc.Decode(data, &m)
	if err != nil {
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
	server.sendResponse(conn, addr, req.h, req.replyv.Interface())
}

func (server *Server) sendResponse(conn *net.UDPConn, addr *net.UDPAddr, h *codec.Header, body interface{}) {
	data, err := server.cc.Encode(h, body)
	if err != nil {
		log.Println("rpc server: encode response error:", err)
		return
	}
	_, err = conn.WriteToUDP(data, addr)
	if err != nil {
		log.Println("rpc server: write response error:", err)
		return
	}
}