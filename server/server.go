package hastyserver

import (
	"log"
	"net"

	"github.com/piot/hasty-protocol/handler"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-server/commandhandler"
	"github.com/piot/hasty-server/connection"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscriber"
	"github.com/piot/hasty-server/subscribers"
	listenserver "github.com/piot/listen-server/server"
)

type HastyServer struct {
	listenServer   listenserver.Server
	commandHandler commandhandler.CommandHandler
	streamStorage  *filestorage.StreamStorage
	subscribers    *subscribers.Subscribers
	master         *master.MasterCommandHandler
}

func NewServer() *HastyServer {
	return &HastyServer{}
}

func setupEnvironment() (*master.MasterCommandHandler, filestorage.StreamStorage, *subscribers.Subscribers, error) {
	storage, storageErr := filestorage.NewFileStorage(".hasty")
	if storageErr != nil {
		return &master.MasterCommandHandler{}, filestorage.StreamStorage{}, nil, storageErr
	}

	streamStorage, streamStorageErr := filestorage.NewStreamStorage(storage)
	if streamStorageErr != nil {
		return &master.MasterCommandHandler{}, filestorage.StreamStorage{}, nil, storageErr
	}

	subs := subscribers.NewSubscribers()

	master := master.NewMasterCommandHandler(&streamStorage, &subs)

	return master, streamStorage, &subs, nil
}

func (in *HastyServer) Listen(host string, cert string, certPrivateKey string) error {
	master, streamStorage, subs, _ := setupEnvironment()
	sub := subscriber.Subscriber{}
	in.subscribers = subs
	in.streamStorage = &streamStorage
	in.master = master
	in.commandHandler = commandhandler.NewCommandHandler(&sub, master)
	in.listenServer = listenserver.NewServer()
	in.listenServer.Listen(in, host, cert, certPrivateKey)
	return nil
}

func (in *HastyServer) CreateConnection(conn *net.Conn, connectionIdentity packet.ConnectionID) (handler.PacketHandler, error) {
	log.Print("HastyServer: CreateConnection")
	delegator := handler.NewPacketHandlerDelegator()
	delegator.AddHandler(in.commandHandler)
	connectionHandler := connection.NewConnectionHandler(conn, in.master, in.streamStorage, in.subscribers, connectionIdentity)
	delegator.AddHandler(connectionHandler)
	return &delegator, nil
}
