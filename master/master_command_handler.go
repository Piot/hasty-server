package master

import (
	"log"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-server/authorization"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
)

// MasterCommandHandler : todo
type MasterCommandHandler struct {
	storage *filestorage.StreamStorage
	subs    *subscribers.Subscribers
}

// NewMasterCommandHandler : todo
func NewMasterCommandHandler(storage *filestorage.StreamStorage, subs *subscribers.Subscribers) MasterCommandHandler {
	return MasterCommandHandler{storage: storage, subs: subs}
}

// HandlePublishStream : todo
func (in *MasterCommandHandler) HandlePublishStream(client authorization.Client, cmd commands.PublishStream) error {
	log.Printf("Master publish:%s", cmd)
	channel := cmd.Channel()
	log.Println("After Channel")
	/*
		authorization, authErr := client.GetChannelAuthorization(channel)
		log.Println("After Channel")
		if authErr != nil {
			return authErr
		}

		if !authorization.AllowedToWrite() {
			return fmt.Errorf("Not allowed to write to %s", channel)
		}
	*/
	streamFile, openErr := in.storage.OpenStream(channel)
	if openErr != nil {
		log.Printf("Couldn't open stream %s", channel)
		return openErr
	}

	appendErr := streamFile.Append(cmd.Chunk())
	if appendErr != nil {
		log.Printf("Couldn't append stream %s", channel)
		return appendErr
	}
	streamFile.Close()
	in.subs.StreamChanged(channel)
	log.Printf("Master publish done")
	return nil
}

// HandleCreateStream : todo
func (in *MasterCommandHandler) HandleCreateStream(client authorization.Client, cmd commands.CreateStream) (channel.ID, error) {
	log.Println("Master createStream:", cmd)
	path := cmd.Path()
	/*
		authorization, authErr := client.GetCreateChannelAuthorization(path)
		if authErr != nil {
			return channel.ID{}, authErr
		}
		if !authorization.AllowedToWrite() {
			return channel.ID{}, fmt.Errorf("Not allowed to write to %s", path)
		}
	*/
	streamFile, channel, newErr := in.storage.NewStream(path)
	if newErr != nil {
		return channel, newErr
	}
	streamFile.Close()
	return channel, nil
}
