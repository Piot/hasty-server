package connection

import (
	"encoding/hex"
	"log"
	"net"
	"strconv"

	"github.com/piot/hasty-protocol/authentication"
	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/chunk"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-protocol/packetserializers"
	"github.com/piot/hasty-protocol/serializer"
	"github.com/piot/hasty-protocol/timestamp"
	"github.com/piot/hasty-protocol/user"
	"github.com/piot/hasty-server/authorization"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
	"github.com/piot/hasty-server/users"
)

const systemChannelID = 0

// StreamInfo : todo
type StreamInfo struct {
	lastOffsetSent uint64
}

// ConnectionHandler : todo
type ConnectionHandler struct {
	conn               *net.Conn
	storage            *filestorage.StreamStorage
	userStorage        *users.Storage
	subscribers        *subscribers.Subscribers
	streamInfos        map[uint32]*StreamInfo
	connectionID       packet.ConnectionID
	masterHandler      *master.MasterCommandHandler
	chunkStreams       map[uint32]*chunk.Stream
	authenticationInfo authentication.Info
}

// NewConnectionHandler : todo
func NewConnectionHandler(connection *net.Conn, masterHandler *master.MasterCommandHandler, storage *filestorage.StreamStorage, userStorage *users.Storage, subs *subscribers.Subscribers, connectionID packet.ConnectionID) *ConnectionHandler {
	return &ConnectionHandler{connectionID: connectionID, masterHandler: masterHandler, conn: connection, storage: storage, userStorage: userStorage, subscribers: subs, streamInfos: map[uint32]*StreamInfo{}, chunkStreams: map[uint32]*chunk.Stream{}}
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

// StreamChanged : todo
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
		lastOffset := 0
		if err == nil {
			offset := v.Offset()
			const maxSizeBuffer = 1 * 1024 * 1024
			buf := make([]byte, maxSizeBuffer)
			readFile.Seek(uint64(offset))
			octetsRead, readErr := readFile.Read(buf)
			if readErr == nil {
				data := buf[:octetsRead]
				lastOffset = int(offset) + octetsRead
				octetsToSend := packetserializers.StreamDataToOctets(v.Channel(), offset, data)
				in.sendPacket(octetsToSend)
			} else {
				log.Printf("Couldn't read. What is that all about? %v", readErr)
			}
		} else {
			log.Printf("Stream %v does not exist yet. You are still subscribed to it", v)
		}
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

func (in *ConnectionHandler) publishMasterStream(channel channel.ID, payload []byte, authenticationInfo authentication.Info) {
	fakeClient := authorization.AdminClient{}
	hexPayload := hex.Dump(payload)
	log.Printf("publishing to channel: %v data: %v", channel, hexPayload)
	authenticationPayload, _ := packetserializers.AuthenticationChunkToOctets(authenticationInfo, payload)
	cmd := commands.NewPublishStream(channel, authenticationPayload)
	in.masterHandler.HandlePublishStream(fakeClient, cmd)
}

func (in *ConnectionHandler) publishNormalStream(channel channel.ID, payload []byte) {
	fakeClient := authorization.AdminClient{}
	hexPayload := hex.Dump(payload)
	log.Printf("publishing to channel: %v data: %v", channel, hexPayload)
	cmd := commands.NewPublishStream(channel, payload)
	in.masterHandler.HandlePublishStream(fakeClient, cmd)
}

func isMasterStream(channelID channel.ID) bool {
	return channelID.Raw() == 3
}

func (in *ConnectionHandler) handleStreamDataForMasterStream(cmd commands.StreamData) {
	log.Printf("Stream Data for Master Stream! %v", cmd)
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
		in.publishMasterStream(cmd.Channel(), foundChunk.Payload(), in.authenticationInfo)
	}
}

func (in *ConnectionHandler) handleStreamDataForNormalStream(cmd commands.StreamData) {
	log.Printf("Stream Data for Normal Stream! %v", cmd)
	in.publishNormalStream(cmd.Channel(), cmd.Data())
}

// HandleStreamData : todo
func (in *ConnectionHandler) HandleStreamData(cmd commands.StreamData) {
	log.Printf("%s %s", in.connectionID, cmd)
	if isMasterStream(cmd.Channel()) {
		in.handleStreamDataForMasterStream(cmd)
	} else {
		in.handleStreamDataForNormalStream(cmd)
	}
}

func convertFromUsernameToUserID(username string) user.ID {
	userIDValue, _ := strconv.ParseUint(username, 10, 64)
	userID, _ := user.NewID(userIDValue)

	return userID
}

// HandleLogin : todo
func (in *ConnectionHandler) HandleLogin(cmd commands.Login) error {
	log.Printf("%s", cmd)
	userID := convertFromUsernameToUserID(cmd.Username())
	userAssignedChannel, userInfoErr := in.userStorage.FindOrCreateUserInfo(userID)
	if userInfoErr != nil {
		log.Printf("ERROR:%v", userInfoErr)
		return userInfoErr
	}
	in.authenticationInfo = authentication.NewInfo(userID, userAssignedChannel)
	in.sendLoginResult(true, userAssignedChannel)

	return nil
}

func (in *ConnectionHandler) publishSystemStream(payload []byte) {
	log.Printf("Publishing to system stream %v", payload)
	channelToPublishTo, _ := channel.NewFromID(systemChannelID)
	in.publishMasterStream(channelToPublishTo, payload, in.authenticationInfo)
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
