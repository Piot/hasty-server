package filestorage

import (
	"log"
	"math/rand"
	"strings"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
)

// StreamStorage : todo
type StreamStorage struct {
	storage FileStorage
	hacks   uint32
}

// NewStreamStorage : Creates a stream storage
func NewStreamStorage(storage FileStorage) (StreamStorage, error) {
	// rand.Seed(time.Now().UnixNano())
	return StreamStorage{storage: storage}, nil
}

func createChannelID(in *StreamStorage) (channel.ID, error) {
	randomStreamID := rand.Uint32()
	randomStreamID = in.hacks
	in.hacks++
	return channel.NewFromID(randomStreamID)
}

func objectPath(channelID channel.ID) opath.OPath {
	streamIDasString := "/objects/@" + channelID.ToHex()
	idPath, _ := opath.NewFromString(streamIDasString)

	return idPath
}

func refPath(path opath.OPath) opath.OPath {
	streamIDasString := "/refs" + path.ToString()
	idPath, _ := opath.NewFromString(streamIDasString)
	return idPath
}

func tryToCreateStream(in *StreamStorage, channelID channel.ID) (AppendFile, error) {
	streamOPath := objectPath(channelID)
	extension := ""
	streamFile, createErr := in.storage.NewFile(streamOPath, extension)
	return streamFile, createErr
}

func createStream(in *StreamStorage) (AppendFile, channel.ID, error) {
	var streamFile AppendFile
	var channelID channel.ID
	for {
		var channelErr error
		channelID, channelErr = createChannelID(in)
		if channelErr != nil {
			return AppendFile{}, channel.ID{}, channelErr
		}
		var createErr error
		streamFile, createErr = tryToCreateStream(in, channelID)
		if createErr == nil {
			break
		}
	}

	return streamFile, channelID, nil
}

// NewStream : creates a new stream
func (in *StreamStorage) NewStream(path opath.OPath) (AppendFile, channel.ID, error) {
	refOPath := refPath(path)
	prepareRefErr := in.storage.WriteAtomic(refOPath, "", []byte(""))
	if prepareRefErr != nil {
		log.Printf("Prepare Ref err:%s", prepareRefErr)
		return AppendFile{}, channel.ID{}, prepareRefErr
	}

	in.hacks = 0
	streamFile, channelID, gaveUpErr := createStream(in)
	if gaveUpErr != nil {
		log.Printf("We gave up:%s", gaveUpErr)
		return AppendFile{}, channelID, gaveUpErr
	}

	refFile, idErr := in.storage.AppendFile(refOPath)
	if idErr != nil {
		log.Printf("Atomic ID err:%s", idErr)
		return AppendFile{}, channel.ID{}, idErr
	}
	refFile.Append([]byte(channelID.ToHex() + "\n"))
	refFile.Close()

	infoPath := objectPath(channelID)
	infoErr := in.storage.WriteAtomic(infoPath, ".info", []byte(path.ToString()+"\n"))
	if infoErr != nil {
		log.Printf("Atomic info err:%s", infoErr)
		return AppendFile{}, channelID, infoErr
	}
	return streamFile, channelID, nil
}

func (in StreamStorage) getInfo(channel channel.ID) (string, error) {
	infoPath := objectPath(channel)
	data := make([]byte, 256)
	octetCount, infoErr := in.storage.ReadAtomic(infoPath, ".info", data)
	if infoErr != nil {
		return "", infoErr
	}
	originalPath := strings.TrimSpace(string(data[:octetCount]))
	return originalPath, nil
}

// OpenStream : opens a new stream
func (in StreamStorage) OpenStream(channel channel.ID) (AppendFile, error) {
	originalPath, infoErr := in.getInfo(channel)
	if infoErr != nil {
		return AppendFile{}, infoErr
	}
	log.Printf("Open Stream %s path: '%s'", channel, originalPath)
	path := objectPath(channel)
	streamFile, streamErr := in.storage.AppendFile(path)
	return streamFile, streamErr
}

// ReadStream : opens a new stream
func (in StreamStorage) ReadStream(channel channel.ID) (ReadFile, error) {
	originalPath, infoErr := in.getInfo(channel)
	if infoErr != nil {
		return ReadFile{}, infoErr
	}
	log.Printf("Read Stream %s path: '%s'", channel, originalPath)
	path := objectPath(channel)
	streamFile, streamErr := in.storage.ReadFile(path, "")
	return streamFile, streamErr
}
