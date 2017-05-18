package realm

import (
	"path"

	"github.com/piot/hasty-protocol/realmname"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscribers"
	"github.com/piot/hasty-server/users"
)

type RealmInfo struct {
	streamStorage *filestorage.StreamStorage
	subs          *subscribers.Subscribers
	masterCommand *master.MasterCommandHandler
	userStorage   *users.Storage
}

func (in *RealmInfo) MasterCommand() *master.MasterCommandHandler {
	return in.masterCommand
}

func (in *RealmInfo) StreamStorage() *filestorage.StreamStorage {
	return in.streamStorage
}

func (in *RealmInfo) UserStorage() *users.Storage {
	return in.userStorage
}

func (in *RealmInfo) Subscribers() *subscribers.Subscribers {
	return in.subs
}

// RealmRoot : todo
type RealmRoot struct {
	realmLookup  map[string]*RealmInfo
	filePathRoot string
}

// NewRealmRoot : Creates a master command root
func NewRealmRoot(filePathRoot string) (*RealmRoot, error) {
	return &RealmRoot{filePathRoot: filePathRoot, realmLookup: map[string]*RealmInfo{}}, nil
}

func (in *RealmRoot) createStreamStorage(realm realmname.Name) (*filestorage.FileStorage, *filestorage.StreamStorage, error) {
	completePath := path.Join(in.filePathRoot, realm.Name()+"/")
	fileStorage, fileStorageErr := filestorage.NewFileStorage(completePath)
	if fileStorageErr != nil {
		return nil, nil, fileStorageErr
	}

	streamStorage, streamStorageErr := filestorage.NewStreamStorage(fileStorage)
	if streamStorageErr != nil {
		return nil, nil, streamStorageErr
	}

	return &fileStorage, streamStorage, nil
}

// GetRealmInfo : returns the master for that realm
func (in *RealmRoot) GetRealmInfo(realm realmname.Name) (*RealmInfo, error) {
	if val, ok := in.realmLookup[realm.Name()]; ok {
		return val, nil
	} else {
		fileStorage, realmStreamStorage, realmStorageErr := in.createStreamStorage(realm)
		if realmStorageErr != nil {
			return nil, realmStorageErr
		}
		userStorage, _ := users.NewStorage(realmStreamStorage, fileStorage)
		subs := subscribers.NewSubscribers()
		masterCommand := master.NewMasterCommandHandler(realmStreamStorage, subs)

		info := &RealmInfo{masterCommand: masterCommand, streamStorage: realmStreamStorage, userStorage: &userStorage, subs: subs}
		in.realmLookup[realm.Name()] = info
		return info, nil
	}
}
