package subscribers

import (
	"container/list"
	"errors"
	"log"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
)

// SubscribeNotify : todo
type SubscribeNotify interface {
	EntityChanged(path opath.OPath)
}

// Subscriber : todo
type Subscriber struct {
	subscribers *list.List
}

// Subscribers : todo
type Subscribers struct {
	pathToSubscribers map[uint32]Subscriber
}

// NewSubscribers : todo
func NewSubscribers() Subscribers {
	pathToSubscribers := Subscribers{}
	pathToSubscribers.pathToSubscribers = make(map[uint32]Subscriber)
	return pathToSubscribers
}

// Check : todo
func (in *Subscribers) Check() {
	if in.pathToSubscribers == nil {
		log.Println("CHECK SUBSCRIBER NULL")
	}
}

// AddSubscriber : todo
func (in *Subscribers) AddSubscriber(c channel.ID, subscribeNotify SubscribeNotify) {
	raw := c.Raw()
	if in.pathToSubscribers == nil {
		log.Println("ADD SUBSCRIBER NULL")
	}
	existingSubscribers := in.pathToSubscribers[raw]
	if existingSubscribers.subscribers == nil {
		existingSubscribers.subscribers = list.New()
		log.Printf("Ex:%p", in.pathToSubscribers)
		in.pathToSubscribers[raw] = existingSubscribers
	}
	existingSubscribers.subscribers.PushFront(subscribeNotify)
	log.Println("All is added")
}

// RemoveSubscriber : Remove a subscriber
func (in *Subscribers) RemoveSubscriber(c channel.ID, subscribeNotify SubscribeNotify) error {
	raw := c.Raw()
	existingSubscribers := in.pathToSubscribers[raw]
	if existingSubscribers.subscribers == nil {
		return errors.New("Couldn't remove subscriber from empty list")
	}
	l := existingSubscribers.subscribers
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == subscribeNotify {
			existingSubscribers.subscribers.Remove(e)
			return nil
		}
	}

	return errors.New("Couldn't remove unknown subscriber")
}
