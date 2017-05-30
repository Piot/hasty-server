package subscriber

import (
	log "github.com/sirupsen/logrus"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-server/subscribers"
)

// Entity : Book-keeping
type Entity struct {
	channelID     channel.ID
	priorityValue int
}

// Subscriber : command
type Subscriber struct {
	subscriptionManager subscribers.Subscribers
	//	subscriptions       map[channel.ID]*Entity
}

// Priority :
type Priority int

// Priority
const (
	Low Priority = 1 + iota
	Mid
	High
)

// NewSubscriber : Creates a subscriber
func NewSubscriber(subscriptionManager subscribers.Subscribers) Subscriber {
	log.Debug("NewSubscriber")
	subscriber := Subscriber{subscriptionManager: subscriptionManager}
	return subscriber
}

func priorityValueFromPriority(priority Priority) int {
	switch priority {
	case High:
		return 100
	case Low:
		return 1
	case Mid:
		return 20
	}
	return 0
}

// Subscribe : start subscribing
func (in *Subscriber) Subscribe(path channel.ID, priority Priority) {
	log.Debugf("Subscribing %s %d", path, priority)
	if in == nil {
		log.Warnf("IN is nil")
	}
	log.Debugf("XXX:%p", in)
}

// UnsubscribeStream : UnsubscribeStream
func (in *Subscriber) UnsubscribeStream(channel channel.ID) {
	log.Debugf("Unsubscribing %s", channel)
	//	in.subscriptionManager.RemoveSubscriber(channel, in)
}
