package filestorage

import (
	"fmt"
	"testing"

	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
)

func writeTestOctets(t *testing.T, streamStorage *StreamStorage, path string) (opath.OPath, channel.ID) {
	opath, opathErr := opath.NewFromString(path)
	if opathErr != nil {
		t.Print(opathErr)
	}
	file, channelID, newFileErr := streamStorage.NewStream(opath)
	if newFileErr != nil {
		fmt.Printf("stream:%s", newFileErr)
		t.Print(newFileErr)
	}
	octets := []byte{1, 2, 3, 4, 5, 42}
	writeErr := file.Write(octets)
	if writeErr != nil {
		t.Print(writeErr)
	}

	file.Close()
	return opath, channelID
}

func TestStreamCreation(t *testing.T) {
	storage, storageErr := NewFileStorage("../temp/.hasty")
	if storageErr != nil {
		t.Print(storageErr)
	}

	streamStorage, streamStorageErr := NewStreamStorage(storage)
	if streamStorageErr != nil {
		t.Print(streamStorageErr)
	}
	writeTestOctets(t, &streamStorage, "/games/@164008/users/13404")

	writeTestOctets(t, &streamStorage, "/games/@164080/users/13404")
	_, channelID := writeTestOctets(t, &streamStorage, "/games/@16408/users/13404")
	reopenFile, openErr := streamStorage.OpenStream(channelID)
	if openErr != nil {
		t.Print(openErr)
	}
	nextChunk := []byte{43, 44, 45, 99}
	reopenFile.Write(nextChunk)
	reopenFile.Close()
}
