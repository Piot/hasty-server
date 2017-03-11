package authorization

import (
	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
)

// AdminClient : todo
type AdminClient struct {
}

// GetCreateChannelAuthorization : todo
func (in AdminClient) GetCreateChannelAuthorization(path opath.OPath) (Authorization, error) {
	return NewAuthorization(true, true), nil
}

// GetChannelAuthorization : todo
func (in AdminClient) GetChannelAuthorization(channel channel.ID) (Authorization, error) {
	return NewAuthorization(true, true), nil
}
