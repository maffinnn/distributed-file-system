package service

import (
	"distributed-file-system/pkg/golang/logger"
	"distributed-file-system/pkg/golang/rpc"
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
	logger  *logger.Logger         //
}

type Subscriber struct {
	Id   string
	Addr string
}

func (s *Subscriber) UpdateFile(args interface{}) {

}

func NewSubscription(logger *logger.Logger) *Subscription {
	return &Subscription{
		Members: make(map[string]*Subscriber),
		logger:  logger,
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
func (sub *Subscription) Broadcast(excludeId string, args interface{}) {
	for id, member := range sub.Members {
		if id == excludeId {
			continue
		}
		conn, err := rpc.Dial(member.Addr, sub.logger)
		if err != nil {
			sub.logger.Printf("[ERROR] subscriber: rpc dial error: %v", err)
			return
		}
		var reply UpdateCallbackPromiseResponse
		if err := conn.Call("FileClient.UpdateCallbackPromise", args, &reply); err != nil {
			sub.logger.Printf("[ERROR] call FileClient.UpdateCallbackPromise error: %v", err)
			return
		}
	}
}
