package users

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/piot/chat-server/user"
	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
	"github.com/piot/hasty-server/storage"
)

// Storage : todo
type Storage struct {
	streamStorage *filestorage.StreamStorage
	fileStorage   *filestorage.FileStorage
}

// NewStorage : Creates a user  storage
func NewStorage(streamStorage *filestorage.StreamStorage, fileStorage *filestorage.FileStorage) (Storage, error) {
	return Storage{streamStorage: streamStorage, fileStorage: fileStorage}, nil
}

func userInfoOPath(userID user.ID) opath.OPath {
	userOPath, _ := opath.NewFromString(fmt.Sprintf("/internal/users/@%d", userID.Raw()))

	return userOPath
}

// FindUserInfo : todo
func (in *Storage) FindUserInfo(userID user.ID) (channel.ID, error) {
	userOPath := userInfoOPath(userID)
	data := make([]byte, 8)
	octetsRead, readErr := in.fileStorage.ReadAtomic(userOPath, ".info", data)
	if readErr != nil {
		return channel.ID{}, readErr
	}
	if octetsRead < 8 {
		return channel.ID{}, fmt.Errorf("Info file is only:%d", octetsRead)
	}
	buffer := data[:8]
	userAssignedChannelIDValue := binary.BigEndian.Uint64(buffer)
	userAssignedChannelID, _ := channel.NewFromID(uint32(userAssignedChannelIDValue))
	log.Printf("Found user info %v channel:%v", userID, userAssignedChannelID)
	return userAssignedChannelID, nil
}

// CreateUserInfo : todo
func (in *Storage) CreateUserInfo(userID user.ID) (channel.ID, error) {
	userInfoPathString := fmt.Sprintf("/internal/userchannel/@%d", userID.Raw())
	log.Printf("CreateUserInfo %v %v", userID, userInfoPathString)

	userChannelOPath, userChannelOPathErr := opath.NewFromString(userInfoPathString)
	if userChannelOPathErr != nil {
		return channel.ID{}, fmt.Errorf("CreateUserInfoErr %v", userChannelOPathErr)
	}
	file, userAssignedChannel, newStreamErr := in.streamStorage.NewStream(userChannelOPath)
	if newStreamErr != nil {
		return channel.ID{}, fmt.Errorf("CreateUserInfoErr: %v", newStreamErr)
	}
	file.Close()

	userOPath := userInfoOPath(userID)
	data := make([]byte, 8)

	binary.BigEndian.PutUint64(data, uint64(userAssignedChannel.Raw()))
	in.fileStorage.WriteAtomic(userOPath, ".info", data)
	log.Printf("Writing user info: %v channel: %v", userID, userAssignedChannel)
	return userAssignedChannel, nil
}

// FindOrCreateUserInfo : todo
func (in *Storage) FindOrCreateUserInfo(userID user.ID) (channel.ID, error) {
	channelID, err := in.FindUserInfo(userID)
	if err != nil {
		return in.CreateUserInfo(userID)
	}
	return channelID, nil
}
