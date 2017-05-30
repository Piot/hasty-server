package hastyserver

import (
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/piot/hasty-protocol/handler"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-server/config"
	"github.com/piot/hasty-server/connection"
	"github.com/piot/hasty-server/realm"
	listenserver "github.com/piot/listen-server/server"
)

type HastyServer struct {
	listenServer listenserver.Server
	hastyConfig  config.HastyConfig
	realmRoot    *realm.RealmRoot
}

func NewServer() *HastyServer {
	return &HastyServer{}
}

func setupEnvironment() (*realm.RealmRoot, error) {
	realmRoot, realmRootErr := realm.NewRealmRoot(".hasty")
	if realmRootErr != nil {
		return nil, realmRootErr
	}

	return realmRoot, nil
}

func (in *HastyServer) Listen(host string, cert string, certPrivateKey string, hastyConfig config.HastyConfig) error {
	realmRoot, _ := setupEnvironment()
	in.realmRoot = realmRoot
	in.hastyConfig = hastyConfig
	in.listenServer = listenserver.NewServer()
	in.listenServer.Listen(in, host, cert, certPrivateKey)
	return nil
}

func (in *HastyServer) CreateConnection(conn *net.Conn, connectionIdentity packet.ConnectionID) (handler.PacketHandler, error) {
	log.Debug("HastyServer: CreateConnection")
	connectionHandler := connection.NewConnectionHandler(conn, in.realmRoot, in.hastyConfig, connectionIdentity)
	return connectionHandler, nil
}
