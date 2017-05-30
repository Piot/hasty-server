package subscribers

import (
	"container/list"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/piot/hasty-protocol/channel"
)

// SubscribeNotify : todo
type SubscribeNotify interface {
	//	EntityChanged(path opath.OPath)
	StreamChanged(id channel.ID)
}

// Subscriber : todo
type Subscriber struct {
	subscribers *list.List
}

// Subscribers : todo
type Subscribers struct {
	channelToSubscribers map[uint32]Subscriber
}

// NewSubscribers : todo
func NewSubscribers() *Subscribers {
	pathToSubscribers := Subscribers{}
	pathToSubscribers.channelToSubscribers = make(map[uint32]Subscriber)
	return &pathToSubscribers
}

// StreamChanged : Called when stream has changed
func (in *Subscribers) StreamChanged(id channel.ID) {
	log.Debugf("Detected a stream change %s", id)
	raw := id.Raw()
	existingSubscribers := in.channelToSubscribers[raw]
	if existingSubscribers.subscribers == nil {
		return
	}
	l := existingSubscribers.subscribers
	for e := l.Front(); e != nil; e = e.Next() {
		log.Debugf("Calling stream changed on subscriber")
		e.Value.(SubscribeNotify).StreamChanged(id)
	}
}

// AddStreamSubscriber : todo
func (in *Subscribers) AddStreamSubscriber(c channel.ID, subscribeNotify SubscribeNotify) error {
	raw := c.Raw()
	if in.channelToSubscribers == nil {
		return fmt.Errorf("channelToSubscribers is nil")
	}
	existingSubscribers := in.channelToSubscribers[raw]
	if existingSubscribers.subscribers == nil {
		existingSubscribers.subscribers = list.New()
		log.Debugf("Ex:%p", in.channelToSubscribers)
		in.channelToSubscribers[raw] = existingSubscribers
	}
	existingSubscribers.subscribers.PushFront(subscribeNotify)
	log.Debugf("Subscriber is added to %s", c)
	return nil
}

// RemoveStreamSubscriber : Remove a subscriber
func (in *Subscribers) RemoveStreamSubscriber(c channel.ID, subscribeNotify *SubscribeNotify) error {
	raw := c.Raw()
	existingSubscribers := in.channelToSubscribers[raw]
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
