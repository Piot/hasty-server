package filestorage

import (
	"fmt"
	"log"
	"os"

	"github.com/piot/hasty-protocol/opath"
)

// AppendFile : todo
type AppendFile struct {
	file *os.File
	name opath.OPath
}

func verifyPosition(file *os.File) error {
	currentOffset, seekErr := file.Seek(0, os.SEEK_CUR)
	if seekErr != nil {
		return seekErr
	}

	fi, statErr := file.Stat()
	if statErr != nil {
		return statErr
	}

	if fi.Size() != currentOffset {
		return fmt.Errorf("We have serious problems. Someone else is changing the file")
	}
	return nil
}

// Append : append octets to file
func (in AppendFile) Append(data []byte) error {
	verifyErr := verifyPosition(in.file)
	if verifyErr != nil {
		return verifyErr
	}
	_, err := in.file.Write(data)
	return err
}

// NewAppendFile : Create append file
func NewAppendFile(file *os.File, name opath.OPath) (AppendFile, error) {
	offset, seekErr := file.Seek(0, os.SEEK_END)
	if seekErr != nil {
		return AppendFile{}, seekErr
	}
	log.Printf("Seeking to end %d of %s", offset, name)
	return AppendFile{file: file, name: name}, nil
}

// Close : Close the file
func (in AppendFile) Close() {
	log.Printf("Close file:%s", in.name)
	in.file.Close()
}
