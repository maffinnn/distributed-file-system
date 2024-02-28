package service

import (
	"log"
	"distributed-file-system/lib/golang/rpc"
)

type Subscription struct {
	Members map[string]*Subscriber // key is the clientid
}

type Subscriber struct {
	Id 	 string
	Addr string
}

func (s *Subscriber) Update() {
	conn, err := rpc.Dial(s.Addr)
	if err != nil {
		log.Printf("subscriber: rpc dial error: %v", err)
		return
	}
	args := &UpdateRequest{}
	var reply UpdateResponse
	if err := conn.Call("FileClient.Update", args, &reply); err != nil {
		log.Printf("call FileClient.Update error: %v", err)
		return
	}
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

func (sub *Subscription) Publish() {
	for _, subscribers := range sub.Members{
		subscribers.Update()
	}
}