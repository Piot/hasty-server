package connection

import (
	"bytes"
	"encoding/hex"
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/piot/hasty-protocol/authentication"
	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/chunk"
	"github.com/piot/hasty-protocol/commands"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-protocol/packetserializers"
	"github.com/piot/hasty-protocol/realmname"
	"github.com/piot/hasty-protocol/serializer"
	"github.com/piot/hasty-protocol/timestamp"
	"github.com/piot/hasty-server/authenticator"
	"github.com/piot/hasty-server/authorization"
	"github.com/piot/hasty-server/config"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/realm"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
	"github.com/piot/hasty-server/users"
)

const systemChannelID = 3

// StreamInfo : todo
type StreamInfo struct {
	lastOffsetSent uint64
}

// ConnectionHandler : todo
type ConnectionHandler struct {
	realm              realmname.Name
	conn               *net.Conn
	storage            *filestorage.StreamStorage
	realmRoot          *realm.RealmRoot
	userStorage        *users.Storage
	subscribers        *subscribers.Subscribers
	streamInfos        map[uint32]*StreamInfo
	connectionID       packet.ConnectionID
	masterHandler      *master.MasterCommandHandler
	chunkStreams       map[uint32]*chunk.Stream
	authenticationInfo authentication.Authenticated
	hastyConfig        config.HastyConfig
}

// NewConnectionHandler : todo
func NewConnectionHandler(connection *net.Conn, realmRoot *realm.RealmRoot, hastyConfig config.HastyConfig, connectionID packet.ConnectionID) *ConnectionHandler {
	return &ConnectionHandler{connectionID: connectionID, realmRoot: realmRoot, conn: connection, hastyConfig: hastyConfig, streamInfos: map[uint32]*StreamInfo{}, chunkStreams: map[uint32]*chunk.Stream{}}
}

// HandleConnect : todo
func (in *ConnectionHandler) HandleConnect(cmd commands.Connect) error {
	log.Debugf("%s %s", in.connectionID, cmd)
	in.realm = cmd.Realm()
	octetsToSend := packetserializers.ConnectResultToOctets()
	in.sendPacket(octetsToSend)
	return nil
}

func (in *ConnectionHandler) sendPong(echoedTime timestamp.Time) {
	log.Debugf("%s sendPong %s", in.connectionID, echoedTime)
	now := timestamp.Now()
	octetsToSend := packetserializers.PongToOctets(now, echoedTime)
	in.sendPacket(octetsToSend)
}

func (in *ConnectionHandler) sendLoginResult(worked bool, channelID channel.ID) {
	log.Debugf("%s sendLoginResult %t", in.connectionID, worked)
	octetsToSend := packetserializers.LoginResultToOctets(worked, channelID)
	in.sendPacket(octetsToSend)
}

func (in *ConnectionHandler) sendCreateStreamResult(request uint64, channelID channel.ID) {
	log.Debugf("%s sendCreateStreamResult %08X %s", in.connectionID, request, channelID)
	octetsToSend := packetserializers.CreateStreamResultToOctets(request, channelID)
	in.sendPacket(octetsToSend)
}

// HandlePing : todo
func (in *ConnectionHandler) HandlePing(cmd commands.Ping) {
	log.Debugf("%s %s", in.connectionID, cmd)
	in.sendPong(cmd.SentTime())
}

// HandlePong : todo
func (in *ConnectionHandler) HandlePong(cmd commands.Pong) {
	now := timestamp.Now()
	latency := now.Raw() - cmd.EchoedTime().Raw()
	log.Debugf("%s Latency: %d ms", in.connectionID, latency)
}

// HandleCreateStream : todo
func (in *ConnectionHandler) HandleCreateStream(cmd commands.CreateStream) (channel.ID, error) {
	log.Debugf("Handle create stream:", cmd)
	createdChannelID, createErr := in.masterHandler.HandleCreateStream(nil, cmd)
	if createErr != nil {
		in.sendCreateStreamResult(cmd.RequestNumber(), channel.ID{})
		return createdChannelID, createErr
	}

	in.sendCreateStreamResult(cmd.RequestNumber(), createdChannelID)
	//	in.subscriber.HandleCreateStream(channel)

	return createdChannelID, nil
}

func (in *ConnectionHandler) HandleCreateStreamResult(cmd commands.CreateStreamResult) error {
	log.Debugf("Handle create stream result: %s", cmd)
	return nil
}

// HandlePublishStreamUser : todo
func (in *ConnectionHandler) HandlePublishStreamUser(cmd commands.PublishStreamUser) error {
	log.Debugf("Handle publish user:%s", cmd)
	channelID, _ := in.userStorage.FindOrCreateUserInfo(cmd.User())
	publishStreamCmd := commands.NewPublishStream(channelID, cmd.Chunk())
	return in.masterHandler.HandlePublishStream(nil, publishStreamCmd)
}

// HandlePublishStream : todo
func (in *ConnectionHandler) HandlePublishStream(cmd commands.PublishStream) error {
	log.Debugf("Handle publish:", cmd)
	return in.masterHandler.HandlePublishStream(nil, cmd)
}

// StreamChanged : todo
func (in *ConnectionHandler) StreamChanged(channelID channel.ID) {
	info := in.fetchOrCreateStreamInfo(channelID)
	in.sendStreamDataFromOffset(channelID, info.lastOffsetSent)
}

func (in *ConnectionHandler) sendStreamDataInChunks(channelID channel.ID, data []byte, octetsRead int, info *StreamInfo) {
	startPos := 0
	const chunkSize int = 4096
	for pos := startPos; pos < octetsRead; pos += chunkSize {
		remaining := octetsRead - pos
		chunkThisTime := chunkSize
		if remaining < chunkSize {
			chunkThisTime = remaining
		}
		in.sendStreamData(channelID, uint32(info.lastOffsetSent), data[pos:pos+chunkThisTime])
		info.lastOffsetSent += uint64(chunkThisTime)
	}
}

func (in *ConnectionHandler) sendStreamData(channelID channel.ID, lastOffsetSent uint32, data []byte) {
	log.Debugf("%s sendStreamData %s offset:%d", in.connectionID, channelID, lastOffsetSent)
	payload := packetserializers.StreamDataToOctets(channelID, lastOffsetSent, data)
	in.sendPacket(payload)
}

func (in *ConnectionHandler) sendStreamDataFromOffset(channelID channel.ID, lastOffset uint64) error {
	info := in.fetchOrCreateStreamInfo(channelID)
	info.lastOffsetSent = lastOffset

	readFile, err := in.storage.ReadStream(channelID)
	if err != nil {
		return err
	}
	readFile.Seek(lastOffset)

	const maxSizeBuffer = 64 * 1024 * 1024
	buf := make([]byte, maxSizeBuffer)
	octetsRead, readErr := readFile.Read(buf)
	if readErr == nil {
		data := buf[:octetsRead]
		in.sendStreamDataInChunks(channelID, data, octetsRead, info)
	} else {
		log.Warnf("Couldn't read. What is that all about? %v", readErr)
	}
	return nil
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
	log.Debugf("%s %s", in.connectionID, cmd)

	for _, v := range cmd.Infos() {
		_, err := in.storage.ReadStream(v.Channel())
		if err == nil {
			in.sendStreamDataFromOffset(v.Channel(), uint64(v.Offset()))
		} else {
			log.Warnf("Stream %v does not exist yet. You are still subscribed to it", v)
		}

		in.subscribers.AddStreamSubscriber(v.Channel(), in)
	}
}

// HandleUnsubscribeStream : todo
func (in *ConnectionHandler) HandleUnsubscribeStream(cmd commands.UnsubscribeStream) {
	log.Debugf("Handle unsubscribe:", cmd)
	//in.masterHandler.UnsubscribeStream(cmd.Channel())
}

func (in *ConnectionHandler) fetchOrAssoicateChunkStream(channelID channel.ID) *chunk.Stream {
	stream := in.chunkStreams[channelID.Raw()]
	if stream == nil {
		stream = chunk.NewChunkStream(in.connectionID, channelID)
		in.chunkStreams[channelID.Raw()] = stream
	}
	return stream
}

func (in *ConnectionHandler) publishMasterStream(channel channel.ID, payload []byte, authenticated authentication.Authenticated) {
	fakeClient := authorization.AdminClient{}
	hexPayload := hex.Dump(payload)
	log.Debugf("publishing to channel: %v data: %v", channel, hexPayload)
	info := authentication.NewInfo(authenticated.UserID())
	authenticationPayload, _ := packetserializers.AuthenticationChunkToOctets(info, payload)
	cmd := commands.NewPublishStream(channel, authenticationPayload)
	in.masterHandler.HandlePublishStream(fakeClient, cmd)
}

func (in *ConnectionHandler) publishNormalStream(channel channel.ID, payload []byte) {
	fakeClient := authorization.AdminClient{}
	hexPayload := hex.Dump(payload)
	log.Debugf("publishing to channel: %v data: %v", channel, hexPayload)
	cmd := commands.NewPublishStream(channel, payload)
	in.masterHandler.HandlePublishStream(fakeClient, cmd)
}

func isMasterStream(channelID channel.ID) bool {
	return channelID.Raw() == 3
}

func (in *ConnectionHandler) handleStreamDataForMasterStream(cmd commands.StreamData) {
	log.Debugf("Stream Data for Master Stream! %v", cmd)
	chunkStream := in.fetchOrAssoicateChunkStream(cmd.Channel())
	chunkStream.Feed(cmd.Data())
	foundChunk, fetchErr := chunkStream.FetchChunk()
	if fetchErr != nil {
		_, isNotDoneError := fetchErr.(*chunk.NotDoneError)
		if isNotDoneError {
		} else {
			log.Warnf("Fetcherror:%s", fetchErr)
		}
	} else {
		in.publishMasterStream(cmd.Channel(), foundChunk.Payload(), in.authenticationInfo)
	}
}

func (in *ConnectionHandler) handleStreamDataForNormalStream(cmd commands.StreamData) {
	log.Debugf("Stream Data for Normal Stream! %v", cmd)
	in.publishNormalStream(cmd.Channel(), cmd.Data())
}

// HandleStreamData : todo
func (in *ConnectionHandler) HandleStreamData(cmd commands.StreamData) {
	log.Debugf("%s %s", in.connectionID, cmd)
	if isMasterStream(cmd.Channel()) {
		in.handleStreamDataForMasterStream(cmd)
	} else {
		in.handleStreamDataForNormalStream(cmd)
	}
}

// HandleLogin : todo
func (in *ConnectionHandler) HandleLogin(cmd commands.Login) error {
	log.Debugf("%s realm: '%s'", cmd, in.realm)

	restAuth := in.hastyConfig.Authentication
	userID, realname, authenticationErr := authenticator.Authenticate(restAuth.URL, restAuth.Path, restAuth.Headers[0].Name, restAuth.Headers[0].Value, cmd.Password())
	if authenticationErr != nil {
		log.Warnf("Error: %v", authenticationErr)
		in.sendLoginResult(false, channel.ID{})
		return authenticationErr
	}

	// User is authenticated, we can bring up the realm-specifics
	realmInfo, realmErr := in.realmRoot.GetRealmInfo(in.realm)
	if realmErr != nil {
		return realmErr
	}
	in.masterHandler = realmInfo.MasterCommand()
	in.storage = realmInfo.StreamStorage()
	in.userStorage = realmInfo.UserStorage()
	in.subscribers = realmInfo.Subscribers()

	userAssignedChannel, userInfoErr := in.userStorage.FindOrCreateUserInfo(userID)
	if userInfoErr != nil {
		log.Warnf("ERROR:%v", userInfoErr)
		return userInfoErr
	}

	in.authenticationInfo = authentication.NewAuthenticated(userID, userAssignedChannel, realname)
	log.Debugf("Logged in:%s", in.authenticationInfo)
	in.sendLoginResult(true, userAssignedChannel)

	authenticatedPayload := packetserializers.AuthenticatedToOctets(in.authenticationInfo)
	in.publishSystemStream(authenticatedPayload)

	return nil
}

// HandleAuthenticated : todo
func (in *ConnectionHandler) HandleAuthenticated(cmd commands.Authenticated) {
}

func (in *ConnectionHandler) publishSystemStream(payload []byte) {
	log.Debugf("Publishing to system stream %v", payload)

	buf := new(bytes.Buffer)
	lengthBuf, lengthErr := serializer.SmallLengthToOctets(uint16(len(payload)))
	if lengthErr != nil {
		log.Warnf("We couldn't write length")
		return
	}
	buf.Write(lengthBuf)
	buf.Write(payload)
	encapsulatedPayload := buf.Bytes()

	channelToPublishTo, _ := channel.NewFromID(systemChannelID)
	in.publishMasterStream(channelToPublishTo, encapsulatedPayload, in.authenticationInfo)
}

func (in *ConnectionHandler) sendPacket(octets []byte) {
	payloadLength := uint16(len(octets))
	hexPayload := hex.Dump(octets)
	lengthBuf, lengthErr := serializer.SmallLengthToOctets(payloadLength)
	if lengthErr != nil {
		log.Warnf("We couldn't write length")
		return
	}
	log.Debugf("%s Sending packet (size %d) %s", in.connectionID, payloadLength, hexPayload)
	(*in.conn).Write(lengthBuf)
	(*in.conn).Write(octets)
}

// HandleTransportDisconnect : todo
func (in *ConnectionHandler) HandleTransportDisconnect() {
	log.Infof("%s Transport disconnect", in.connectionID)
}
