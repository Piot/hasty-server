package subscriber

import (
	"log"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
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
	subscriptions       map[channel.ID]*Entity
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
	log.Println("NewSubscriber")
	subscriptions := map[channel.ID]*Entity{}
	subscriber := Subscriber{subscriptionManager: subscriptionManager, subscriptions: subscriptions}
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
	log.Printf("Subscribing %s %d", path, priority)
	if in == nil {
		log.Println("IN is nil")
	}
	log.Printf("XXX:%p", in)
	existingEntry := in.subscriptions[path]
	if existingEntry == nil {
		existingEntry = new(Entity)
		existingEntry.channelID = path
	}
	existingEntry.priorityValue = priorityValueFromPriority(priority)

	// in.subscriptionManager.AddSubscriber(path, in)
}

// UnsubscribeStream : UnsubscribeStream
func (in *Subscriber) UnsubscribeStream(channel channel.ID) {
	log.Printf("Unsubscribing %s", channel)
	in.subscriptionManager.RemoveSubscriber(channel, in)
}

// EntityChanged : Entity has changed
func (in *Subscriber) EntityChanged(path opath.OPath) {
	log.Printf("Entity changed:%s", path)
}

// FetchNext : Fetches the next entity
func (in *Subscriber) FetchNext() *Entity {
	for _, value := range in.subscriptions {
		return value
	}
	return nil
}
