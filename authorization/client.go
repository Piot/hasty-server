package authorization

import (
	"github.com/piot/hasty-protocol/channel"
	"github.com/piot/hasty-protocol/opath"
)

// Client : todo
type Client interface {
	GetCreateChannelAuthorization(path opath.OPath) (Authorization, error)
	GetChannelAuthorization(channel channel.ID) (Authorization, error)
}
