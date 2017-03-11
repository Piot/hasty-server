package commandhandler

import (
	"log"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/subscriber"
)

// CommandHandler : todo
type CommandHandler struct {
	subscriber *subscriber.Subscriber
	master     *master.MasterCommandHandler
}

// NewCommandHandler : todo
func NewCommandHandler(subscriber *subscriber.Subscriber, master *master.MasterCommandHandler) CommandHandler {
	return CommandHandler{subscriber: subscriber, master: master}
}

// HandleConnect : todo
func (in CommandHandler) HandleConnect(cmd commands.Connect) error {
	log.Println("Handle connect:", cmd)
	return nil
}

// HandlePublishStream : todo
func (in CommandHandler) HandlePublishStream(cmd commands.PublishStream) error {
	log.Println("Handle publish:", cmd)
	return in.master.HandlePublishStream(nil, cmd)
}

// HandleSubscribeStream : todo
func (in CommandHandler) HandleSubscribeStream(cmd commands.SubscribeStream) {
	log.Println("Handle subscribe:", cmd)
	if in.subscriber == nil {
		log.Println("GARHGH")
	}
	// in.subscriber.Subscribe(cmd.ChannelID(), subscriber.High)
}

// HandleUnsubscribeStream : todo
func (in CommandHandler) HandleUnsubscribeStream(cmd commands.UnsubscribeStream) {
	log.Println("Handle unsubscribe:", cmd)
	in.subscriber.UnsubscribeStream(cmd.Channel())
}

// HandleCreateStream : todo
func (in CommandHandler) HandleCreateStream(cmd commands.CreateStream) (channel.ID, error) {
	log.Println("Handle create stream:", cmd)
	channel, createErr := in.master.HandleCreateStream(nil, cmd)
	if createErr != nil {
		return channel, createErr
	}

	//	in.subscriber.HandleCreateStream(channel)

	return channel, nil
}

// HandleStreamData : todo
func (in CommandHandler) HandleStreamData(cmd commands.StreamData) {
}
