package subscribe

import "time"

type Subscription struct {
	members map[string]*Subscriber
}

type Subscriber struct {
	ClientId string
	timeout context.Context
}


func NewSubscriber(clientId string, timeout time.Duration) *Subscriber {
	return &Subscriber{

	}
}