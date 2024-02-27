package subscribe

type Subscription struct {
	Members map[string]*Subscriber // key is the clientid
}

type Subscriber struct {
	Addr string
}

func NewSubscription() *Subscription {
	return &Subscription{
		Members: make(map[string]*Subscriber),
	}
}

func (sub *Subscription) Subscribe(clientId, clientAddr string) {
	sub.Members[clientId] = &Subscriber{
		Addr: clientAddr,
	}
}

func (sub *Subscription) Unsubscribe(clientId string) {
	delete(sub.Members, clientId)
}

func (sub *Subscription) Publish() {

}