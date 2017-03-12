package connection

import (
	"log"
	"net"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-protocol/serializer"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
)

type StreamInfo struct {
	lastOffsetSent uint64
}

type ConnectionHandler struct {
	conn        *net.Conn
	storage     *filestorage.StreamStorage
	subscribers *subscribers.Subscribers
	streamInfos map[uint32]*StreamInfo
}

// NewConnectionHandler : todo
func NewConnectionHandler(connection *net.Conn, storage *filestorage.StreamStorage, subs *subscribers.Subscribers) ConnectionHandler {
	return ConnectionHandler{conn: connection, storage: storage, subscribers: subs, streamInfos: map[uint32]*StreamInfo{}}
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

func (in *ConnectionHandler) StreamChanged(channelID channel.ID) {
	info := in.streamInfos[channelID.Raw()]
	file, fileErr := in.storage.ReadStream(channelID)
	if fileErr != nil {
		return
	}
	file.Seek(info.lastOffsetSent)
	data := make([]byte, 32*1024)
	octetsRead, readErr := file.Read(data)
	if readErr != nil {
		return
	}
	file.Close()
	payload := serializer.StreamDataToOctets(channelID, uint32(info.lastOffsetSent), data[:octetsRead])
	info.lastOffsetSent += uint64(octetsRead)
	in.sendPacket(payload)
}

func (in *ConnectionHandler) fetchOrCreateStreamInfo(channelID channel.ID) *StreamInfo {
	infos := in.streamInfos[channelID.Raw()]
	if infos == nil {
		infos = &StreamInfo{}
		in.streamInfos[channelID.Raw()] = infos
	}
	return infos
}

// HandleSubscribeStream : todo
func (in *ConnectionHandler) HandleSubscribeStream(cmd commands.SubscribeStream) {
	log.Println("Connection subscribe:", cmd)

	for _, v := range cmd.Infos() {
		readFile, err := in.storage.ReadStream(v.Channel())

		if err != nil {
			return
		}

		buf := make([]byte, 64*1024)
		octetsRead, readErr := readFile.Read(buf)
		if readErr != nil {
			return
		}
		data := buf[:octetsRead]

		octetsToSend := serializer.StreamDataToOctets(v.Channel(), 0, data)
		in.sendPacket(octetsToSend)
		infos := in.fetchOrCreateStreamInfo(v.Channel())
		infos.lastOffsetSent = uint64(octetsRead)
		in.subscribers.AddStreamSubscriber(v.Channel(), in)
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
	lengthBuf, lengthErr := serializer.SmallLengthToOctets(uint16(len(octets)))
	if lengthErr != nil {
		log.Fatalf("We couldn't write length")
		return
	}
	(*in.conn).Write(lengthBuf)
	(*in.conn).Write(octets)
}
