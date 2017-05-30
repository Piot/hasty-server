package filestorage

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"github.com/piot/hasty-protocol/opath"
	"github.com/piot/hasty-server/ofilepath"
)

// FileStorage : todo
type FileStorage struct {
	prefix string
}

// Nop : The different types
const (
	DefaultDirectoryPermission = 0755
	DefaultFilePermission      = 0644
)

// NewFileStorage : Creates a file storage
func NewFileStorage(prefix string) (FileStorage, error) {
	absolutePath, err := filepath.Abs(prefix)
	if err != nil {
		return FileStorage{}, err
	}
	return FileStorage{prefix: absolutePath}, nil
}

func (in FileStorage) opathToFullPath(opath opath.OPath) (string, error) {
	ofp, ofpErr := ofilepath.NewFromOPath(opath)
	if ofpErr != nil {
		return "", ofpErr
	}
	completePath := path.Join(in.prefix, ofp.ToString())
	return completePath, nil
}

func openFile(completePath string, flags int) (*os.File, error) {
	fileHandle, err := os.OpenFile(completePath, flags, DefaultFilePermission)
	if err != nil {
		return nil, err
	}

	return fileHandle, nil
}

// NewFile : Creates a new file
func (in FileStorage) NewFile(opath opath.OPath, extension string) (AppendFile, error) {
	log.Debugf("NewFile '%v'", opath)
	completePath, completeErr := in.opathToFullPath(opath)
	if len(extension) > 0 {
		completePath += extension
	}
	if completeErr != nil {
		return AppendFile{}, completeErr
	}
	directory := path.Dir(completePath)
	mkdirErr := os.MkdirAll(directory, DefaultDirectoryPermission)
	if mkdirErr != nil {
		return AppendFile{}, fmt.Errorf("NewFile err: '%v'", mkdirErr)
	}

	fileHandle, openErr := openFile(completePath, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	if openErr != nil {
		return AppendFile{}, fmt.Errorf("NewFile Create err '%v'", openErr)
	}
	return NewAppendFile(fileHandle, opath)
}

// AppendFile : Opens an existing file
func (in FileStorage) AppendFile(opath opath.OPath) (AppendFile, error) {
	log.Debugf("Open append %s", opath.ToString())

	completePath, completeErr := in.opathToFullPath(opath)
	if completeErr != nil {
		return AppendFile{}, completeErr
	}
	fileHandle, openErr := openFile(completePath, os.O_RDWR)
	if openErr != nil {
		return AppendFile{}, openErr
	}
	return NewAppendFile(fileHandle, opath)
}

// ReadFile : Opens an existing file
func (in FileStorage) ReadFile(opath opath.OPath, extension string) (ReadFile, error) {
	// log.Debugf("Read file %s (extension:%s)", opath.ToString(), extension)
	completePath, completeErr := in.opathToFullPath(opath)
	if len(extension) > 0 {
		completePath += extension
	}
	if completeErr != nil {
		return ReadFile{}, completeErr
	}
	fileHandle, openErr := openFile(completePath, os.O_RDWR)
	if openErr != nil {
		return ReadFile{}, openErr
	}
	return ReadFile{file: fileHandle}, nil
}

// WriteAtomic : Writes a file atomically
func (in FileStorage) WriteAtomic(opath opath.OPath, extension string, data []byte) error {
	// log.Debugf("Write atomic %s (extension:%s)", opath.ToString(), extension)
	file, createErr := in.NewFile(opath, extension)
	if createErr != nil {
		return createErr
	}
	file.Append(data)
	file.Close()
	return nil
}

// ReadAtomic : Reads a file atomically
func (in FileStorage) ReadAtomic(opath opath.OPath, extension string, data []byte) (int, error) {
	// log.Debugf("ReadAtomic:%s extension:%s", opath, extension)
	file, createErr := in.ReadFile(opath, extension)
	if createErr != nil {
		log.Warnf("Open error:%s", createErr)
		return 0, createErr
	}
	octetCount, readErr := file.Read(data)
	if readErr != nil {
		log.Warnf("Read error:%s", readErr)
		return 0, readErr
	}
	file.Close()
	return octetCount, nil
}
