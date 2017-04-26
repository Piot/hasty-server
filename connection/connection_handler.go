package connection

import (
	"encoding/hex"
	"log"
	"net"

	"github.com/piot/hasty-protocol/authentication"
	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/chunk"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-protocol/packetserializers"
	"github.com/piot/hasty-protocol/serializer"
	"github.com/piot/hasty-protocol/timestamp"
	"github.com/piot/hasty-server/authorization"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
)

const systemChannelID = 0

type StreamInfo struct {
	lastOffsetSent uint64
}

type ConnectionHandler struct {
	conn               *net.Conn
	storage            *filestorage.StreamStorage
	subscribers        *subscribers.Subscribers
	streamInfos        map[uint32]*StreamInfo
	connectionID       packet.ConnectionID
	masterHandler      *master.MasterCommandHandler
	chunkStreams       map[uint32]*chunk.Stream
	authenticationInfo authentication.Info
}

// NewConnectionHandler : todo
func NewConnectionHandler(connection *net.Conn, masterHandler *master.MasterCommandHandler, storage *filestorage.StreamStorage, subs *subscribers.Subscribers, connectionID packet.ConnectionID) *ConnectionHandler {
	return &ConnectionHandler{connectionID: connectionID, masterHandler: masterHandler, conn: connection, storage: storage, subscribers: subs, streamInfos: map[uint32]*StreamInfo{}, chunkStreams: map[uint32]*chunk.Stream{}}
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

func (in *ConnectionHandler) sendLoginResult(worked bool, channelID channel.ID) {
	log.Printf("%s sendLoginResult %t", in.connectionID, worked)
	octetsToSend := packetserializers.LoginResultToOctets(channelID)
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
		offset := v.Offset()

		const maxSizeBuffer = 1 * 1024 * 1024
		buf := make([]byte, maxSizeBuffer)
		readFile.Seek(uint64(offset))
		octetsRead, readErr := readFile.Read(buf)
		if readErr != nil {
			return
		}
		data := buf[:octetsRead]
		lastOffset := int(offset) + octetsRead
		octetsToSend := packetserializers.StreamDataToOctets(v.Channel(), offset, data)
		in.sendPacket(octetsToSend)
		infos := in.fetchOrCreateStreamInfo(v.Channel())
		infos.lastOffsetSent = uint64(lastOffset)
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

func (in *ConnectionHandler) fetchOrAssoicateChunkStream(channelID channel.ID) *chunk.Stream {
	stream := in.chunkStreams[channelID.Raw()]
	if stream == nil {
		stream = chunk.NewChunkStream(in.connectionID, channelID)
		in.chunkStreams[channelID.Raw()] = stream
	}
	return stream
}

func (in *ConnectionHandler) publishMasterStream(channel channel.ID, payload []byte) {
	fakeClient := authorization.AdminClient{}
	log.Printf("publishing to channel: %v data: %v", channel, payload)
	cmd := commands.NewPublishStream(channel, payload)
	in.masterHandler.HandlePublishStream(fakeClient, cmd)
}

// HandleStreamData : todo
func (in *ConnectionHandler) HandleStreamData(cmd commands.StreamData) {
	log.Printf("%s %s", in.connectionID, cmd)
	chunkStream := in.fetchOrAssoicateChunkStream(cmd.Channel())
	chunkStream.Feed(cmd.Data())
	foundChunk, fetchErr := chunkStream.FetchChunk()
	if fetchErr != nil {
		_, isNotDoneError := fetchErr.(*chunk.NotDoneError)
		if isNotDoneError {
		} else {
			log.Printf("Fetcherror:%s", fetchErr)
		}
	} else {
		in.publishMasterStream(cmd.Channel(), foundChunk.Payload())
	}
}

// HandleLogin : todo
func (in *ConnectionHandler) HandleLogin(cmd commands.Login) error {
	log.Printf("%s", cmd)
	/*	userInfo, userInfoErr := users.FindOrCreateUserInfo(cmd.Username())
		if userInfoErr != nil {
			return userInfoErr
		}
		in.sendLoginResult(true, userInfo.Channel())
	*/
	return nil
}

func (in *ConnectionHandler) publishSystemStream(payload []byte) {
	log.Printf("Publishing to system stream %v", payload)
	channelToPublishTo, _ := channel.NewFromID(systemChannelID)
	in.publishMasterStream(channelToPublishTo, payload)
}

func (in *ConnectionHandler) sendPacket(octets []byte) {
	payloadLength := uint16(len(octets))
	hexPayload := hex.Dump(octets)
	lengthBuf, lengthErr := serializer.SmallLengthToOctets(payloadLength)
	if lengthErr != nil {
		log.Printf("We couldn't write length")
		return
	}
	log.Printf("%s Sending packet (size %d) %s", in.connectionID, payloadLength, hexPayload)
	(*in.conn).Write(lengthBuf)
	(*in.conn).Write(octets)
}

// HandleTransportDisconnect : todo
func (in *ConnectionHandler) HandleTransportDisconnect() {
	log.Printf("%s Transport disconnect", in.connectionID)
}
