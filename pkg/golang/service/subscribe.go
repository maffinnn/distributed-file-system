package service

import (
	"distributed-file-system/pkg/golang/rpc"
	"log"
)

type Topic string

const (
	FileUpdateTopic      Topic = "FileUpdate"
	DirectoryUpdateTopic Topic = "DirectoryUpdate"
)

// valid or cancelled
type CallbackPromise struct {
	ValidOrCanceled bool // true if it is valid, false otherwise
}

func NewCallbackPromise() *CallbackPromise {
	return &CallbackPromise{
		ValidOrCanceled: true,
	}
}

func (cp *CallbackPromise) IsValid() bool { return cp.ValidOrCanceled }

func (cp *CallbackPromise) IsCanceled() bool { return !cp.ValidOrCanceled }

func (cp *CallbackPromise) Validate() { cp.ValidOrCanceled = true }

func (cp *CallbackPromise) Cancel() { cp.ValidOrCanceled = false }

func (cp *CallbackPromise) Set(value bool) { cp.ValidOrCanceled = value }

type Subscription struct {
	Members map[string]*Subscriber // key is the clientid
}

type Subscriber struct {
	Id   string
	Addr string
}

func (s *Subscriber) UpdateFile(args interface{}) {
	conn, err := rpc.Dial(s.Addr)
	if err != nil {
		log.Printf("subscriber: rpc dial error: %v", err)
		return
	}
	var reply UpdateCallbackPromiseResponse
	if err := conn.Call("FileClient.UpdateCallbackPromise", args, &reply); err != nil {
		log.Printf("call FileClient.UpdateCallbackPromise error: %v", err)
		return
	}
}

func (s *Subscriber) UpdateDirectory(args interface{}) {
	// TODO:
}

func NewSubscription() *Subscription {
	return &Subscription{
		Members: make(map[string]*Subscriber),
	}
}

func (sub *Subscription) Subscribe(clientId, clientAddr string) {
	if clientId == "" || clientAddr == "" {
		return
	}
	sub.Members[clientId] = &Subscriber{
		Id:   clientId,
		Addr: clientAddr,
	}
}

func (sub *Subscription) Unsubscribe(clientId string) {
	delete(sub.Members, clientId)
}

// excludeId is the client to be excluded from this update
func (sub *Subscription) Broadcast(excludeId string, topic Topic, args interface{}) {
	switch topic {
	case FileUpdateTopic:
		for id, subscribers := range sub.Members {
			if id == excludeId {
				continue
			}
			subscribers.UpdateFile(args)
		}
	case DirectoryUpdateTopic:
		for id, subscribers := range sub.Members {
			if id == excludeId {
				continue
			}
			subscribers.UpdateDirectory(args)
		}
	}
}
