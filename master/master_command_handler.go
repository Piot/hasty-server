package master

import (
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-server/authorization"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
)

// MasterCommandHandler : todo
type MasterCommandHandler struct {
	storage           *filestorage.StreamStorage
	subs              *subscribers.Subscribers
	publishStreamLock *sync.Mutex
}

// NewMasterCommandHandler : todo
func NewMasterCommandHandler(storage *filestorage.StreamStorage, subs *subscribers.Subscribers) *MasterCommandHandler {
	return &MasterCommandHandler{storage: storage, subs: subs, publishStreamLock: &sync.Mutex{}}
}

// HandlePublishStream : todo
func (in *MasterCommandHandler) HandlePublishStream(client authorization.Client, cmd commands.PublishStream) error {
	channel := cmd.Channel()
	in.publishStreamLock.Lock()
	defer in.publishStreamLock.Unlock()
	log.Debugf("Master publish:%s", cmd)
	streamFile, openErr := in.storage.OpenStream(channel)
	if openErr != nil {
		log.Warnf("Couldn't open stream %s", channel)
		return openErr
	}

	appendErr := streamFile.Append(cmd.Chunk())
	if appendErr != nil {
		log.Warnf("Couldn't append stream %s", channel)
		return appendErr
	}
	streamFile.Close()
	in.subs.StreamChanged(channel)
	log.Debugf("Master publish done")
	return nil
}

// HandleCreateStream : todo
func (in *MasterCommandHandler) HandleCreateStream(client authorization.Client, cmd commands.CreateStream) (channel.ID, error) {
	log.Debugf("Master createStream:", cmd)
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
