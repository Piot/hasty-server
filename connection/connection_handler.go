package connection

import (
	"log"
	"net"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-protocol/packetserializers"
	"github.com/piot/hasty-protocol/serializer"
	"github.com/piot/hasty-protocol/timestamp"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
)

type StreamInfo struct {
	lastOffsetSent uint64
}

type ConnectionHandler struct {
	conn         *net.Conn
	storage      *filestorage.StreamStorage
	subscribers  *subscribers.Subscribers
	streamInfos  map[uint32]*StreamInfo
	connectionID packet.ConnectionID
}

// NewConnectionHandler : todo
func NewConnectionHandler(connection *net.Conn, storage *filestorage.StreamStorage, subs *subscribers.Subscribers, connectionID packet.ConnectionID) *ConnectionHandler {
	return &ConnectionHandler{connectionID: connectionID, conn: connection, storage: storage, subscribers: subs, streamInfos: map[uint32]*StreamInfo{}}
}

// HandleConnect : todo
func (in *ConnectionHandler) HandleConnect(cmd commands.Connect) error {
	log.Printf("%s %s", in.connectionID, cmd)

	// _ := commands.NewConnectResult(cmd.Realm(), cmd.ProtocolVersion())
	octetsToSend := packetserializers.ConnectResultToOctets()
	in.sendPacket(octetsToSend)
	return nil
}

func (in *ConnectionHandler) sendPong(echoedTime timestamp.Time) {
	log.Printf("%s sendPong %s", in.connectionID, echoedTime)
	now := timestamp.Now()
	octetsToSend := packetserializers.PongToOctets(now, echoedTime)
	in.sendPacket(octetsToSend)
}

// HandlePing : todo
func (in *ConnectionHandler) HandlePing(cmd commands.Ping) {
	log.Printf("%s %s", in.connectionID, cmd)
	in.sendPong(cmd.SentTime())
}

// HandlePong : todo
func (in *ConnectionHandler) HandlePong(cmd commands.Pong) {
	now := timestamp.Now()
	latency := now.Raw() - cmd.EchoedTime().Raw()
	log.Printf("%s Latency: %d ms", in.connectionID, latency)
}

// HandlePublishStream : todo
func (in *ConnectionHandler) HandlePublishStream(cmd commands.PublishStream) error {
	log.Printf("%s %s", in.connectionID, cmd)
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
	in.sendStreamData(channelID, uint32(info.lastOffsetSent), data[:octetsRead])
	info.lastOffsetSent += uint64(octetsRead)
}

func (in *ConnectionHandler) sendStreamData(channelID channel.ID, lastOffsetSent uint32, data []byte) {
	log.Printf("%s sendStreamData %s offset:%d", in.connectionID, channelID, lastOffsetSent)
	payload := packetserializers.StreamDataToOctets(channelID, lastOffsetSent, data)
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
	log.Printf("%s %s", in.connectionID, cmd)

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

		octetsToSend := packetserializers.StreamDataToOctets(v.Channel(), 0, data)
		in.sendPacket(octetsToSend)
		infos := in.fetchOrCreateStreamInfo(v.Channel())
		infos.lastOffsetSent = uint64(octetsRead)
		in.subscribers.AddStreamSubscriber(v.Channel(), in)
	}
}

// HandleUnsubscribeStream : todo
func (in *ConnectionHandler) HandleUnsubscribeStream(cmd commands.UnsubscribeStream) {
	log.Printf("%s %s", in.connectionID, cmd)
}

// HandleCreateStream : todo
func (in *ConnectionHandler) HandleCreateStream(cmd commands.CreateStream) (channel.ID, error) {
	log.Printf("%s %s", in.connectionID, cmd)
	return channel.ID{}, nil
}

// HandleStreamData : todo
func (in *ConnectionHandler) HandleStreamData(cmd commands.StreamData) {
	log.Printf("%s %s", in.connectionID, cmd)
}

func (in *ConnectionHandler) sendPacket(octets []byte) {
	payloadLength := uint16(len(octets))
	log.Printf("%s Sending packet size: %d", in.connectionID, payloadLength)
	lengthBuf, lengthErr := serializer.SmallLengthToOctets(payloadLength)
	if lengthErr != nil {
		log.Printf("We couldn't write length")
		return
	}
	(*in.conn).Write(lengthBuf)
	(*in.conn).Write(octets)
}
