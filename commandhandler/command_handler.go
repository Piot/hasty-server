package commandhandler

import (
	"log"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/subscriber"
	"github.com/piot/hasty-server/users"
)

// CommandHandler : todo
type CommandHandler struct {
	subscriber  *subscriber.Subscriber
	master      *master.MasterCommandHandler
	userStorage *users.Storage
}

// NewCommandHandler : todo
func NewCommandHandler(subscriber *subscriber.Subscriber, master *master.MasterCommandHandler, userStorage *users.Storage) CommandHandler {
	return CommandHandler{subscriber: subscriber, master: master, userStorage: userStorage}
}

// HandleConnect : todo
func (in CommandHandler) HandleConnect(cmd commands.Connect) error {
	log.Println("Handle connect:", cmd)
	return nil
}

// HandlePing : todo
func (in CommandHandler) HandlePing(cmd commands.Ping) {
}

// HandlePong : todo
func (in CommandHandler) HandlePong(cmd commands.Pong) {
}

// HandlePublishStreamUser : todo
func (in CommandHandler) HandlePublishStreamUser(cmd commands.PublishStreamUser) error {
	log.Println("Handle publish user:", cmd)
	channelID, _ := in.userStorage.FindOrCreateUserInfo(cmd.User())
	publishStreamCmd := commands.NewPublishStream(channelID, cmd.Chunk())
	return in.master.HandlePublishStream(nil, publishStreamCmd)
}

// HandlePublishStream : todo
func (in CommandHandler) HandlePublishStream(cmd commands.PublishStream) error {
	log.Println("Handle publish:", cmd)
	return in.master.HandlePublishStream(nil, cmd)
}

// HandleSubscribeStream : todo
func (in CommandHandler) HandleSubscribeStream(cmd commands.SubscribeStream) {
	log.Println("Handle subscribe:", cmd)
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

// HandleLogin : todo
func (in CommandHandler) HandleLogin(cmd commands.Login) error {
	return nil
}

// HandleAuthenticated : todo
func (in CommandHandler) HandleAuthenticated(cmd commands.Authenticated) {
}

// HandleTransportDisconnect : todo
func (in CommandHandler) HandleTransportDisconnect() {
}
