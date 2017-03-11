package filestorage

import (
	"testing"

	"github.com/piot/hasty-protocol/opath"
)

func TestFileCreation(t *testing.T) {
	storage, storageErr := NewFileStorage("../temp")
	if storageErr != nil {
		t.Error(storageErr)
	}

	opath, opathErr := opath.NewFromString("/files/@999/users/13404")
	if opathErr != nil {
		t.Fatal(opathErr)
	}

	file, newFileErr := storage.NewFile(opath, "")
	if newFileErr != nil {
		t.Fatal(newFileErr)
	}
	octets := []byte{1, 2, 3, 4, 5, 42}
	writeErr := file.Write(octets)
	if writeErr != nil {
		t.Fatal(writeErr)
	}

	file.Close()

	reopenFile, openErr := storage.AppendFile(opath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	nextChunk := []byte{43, 44, 45, 99}
	reopenFile.Write(nextChunk)
	reopenFile.Close()
}
