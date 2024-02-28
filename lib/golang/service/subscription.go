package service

import (
	"distributed-file-system/lib/golang/rpc"
	"log"
)

type Topic string

const (
	FileUpdateTopic      Topic = "FileUpdate"
	DirectoryUpdateTopic Topic = "DirectoryUpdate"
)

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
	var reply UpdateFileResponse
	if err := conn.Call("FileClient.UpdateFile", args, &reply); err != nil {
		log.Printf("call FileClient.UpdateFile error: %v", err)
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
	sub.Members[clientId] = &Subscriber{
		Id:   clientId,
		Addr: clientAddr,
	}
}

func (sub *Subscription) Unsubscribe(clientId string) {
	delete(sub.Members, clientId)
}

// excludeId is the client to be excluded from this update
func (sub *Subscription) Publish(excludeId string, topic Topic, args interface{}) {
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
