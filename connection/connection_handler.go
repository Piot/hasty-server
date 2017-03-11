package connection

import (
	"log"
	"net"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-protocol/serializer"
	"github.com/piot/hasty-server/storage"
)

type ConnectionHandler struct {
	conn    *net.Conn
	storage *filestorage.StreamStorage
}

// NewConnectionHandler : todo
func NewConnectionHandler(connection *net.Conn, storage *filestorage.StreamStorage) ConnectionHandler {
	return ConnectionHandler{conn: connection, storage: storage}
}

// HandleConnect : todo
func (in ConnectionHandler) HandleConnect(cmd commands.Connect) error {
	log.Println("Connection connect:", cmd)

	// _ := commands.NewConnectResult(cmd.Realm(), cmd.ProtocolVersion())
	octetsToSend := serializer.ConnectResultToOctets()
	in.sendPacket(octetsToSend)
	return nil
}

// HandlePublishStream : todo
func (in ConnectionHandler) HandlePublishStream(cmd commands.PublishStream) error {
	log.Println("Connection publish:", cmd)
	return nil
}

// HandleSubscribeStream : todo
func (in ConnectionHandler) HandleSubscribeStream(cmd commands.SubscribeStream) {
	log.Println("Connection subscribe:", cmd)

	for _, v := range cmd.Infos() {
		readFile, err := in.storage.ReadStream(v.Channel())

		if err != nil {
			return
		}

		buf := make([]byte, 1024)
		octetsRead, readErr := readFile.Read(buf)
		if readErr != nil {
			return
		}
		data := buf[:octetsRead]

		octetsToSend := serializer.StreamDataToOctets(v.Channel(), 0, data)
		in.sendPacket(octetsToSend)
	}

}

// HandleUnsubscribeStream : todo
func (in ConnectionHandler) HandleUnsubscribeStream(cmd commands.UnsubscribeStream) {
	log.Println("Connection unsubscribe:", cmd)
}

// HandleCreateStream : todo
func (in ConnectionHandler) HandleCreateStream(cmd commands.CreateStream) (channel.ID, error) {
	log.Println("Connection create stream:", cmd)
	return channel.ID{}, nil
}

// HandleStreamData : todo
func (in ConnectionHandler) HandleStreamData(cmd commands.StreamData) {
	log.Println("Connection stream data:", cmd)
}

func (in *ConnectionHandler) sendPacket(octets []byte) {
	header := []byte{byte(len(octets))}
	(*in.conn).Write(header)
	(*in.conn).Write(octets)
}
