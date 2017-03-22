package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"

	"github.com/piot/hasty-protocol/handler"
	"github.com/piot/hasty-protocol/opath"
	"github.com/piot/hasty-protocol/packet"
	"github.com/piot/hasty-protocol/packetdeserializers"
	"github.com/piot/hasty-server/commandhandler"
	"github.com/piot/hasty-server/connection"
	"github.com/piot/hasty-server/master"
	"github.com/piot/hasty-server/storage"
	"github.com/piot/hasty-server/subscriber"
	"github.com/piot/hasty-server/subscribers"
)

const (
	CONN_TYPE = "tcp"
)

type NullSubscribers struct {
}

func (in NullSubscribers) EntityChanged(path opath.OPath) {

}

type Server struct {
	nextConnectionIdentity uint
}

func NewServer() Server {
	return Server{}
}

func setupCert(cfg *tls.Config, cert string, certPrivateKey string) error {

	cfg.RootCAs = x509.NewCertPool()
	ca, err := ioutil.ReadFile("certs/ca.pem")
	if err == nil {
		fmt.Printf("CA!")
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}

	keyPair, err := tls.LoadX509KeyPair(cert, certPrivateKey)
	if err != nil {
		log.Printf("server: loadkeys: %s", err)
		return err
	}
	cfg.Certificates = append(cfg.Certificates, keyPair)

	return nil
}

func setupEnvironment() (master.MasterCommandHandler, filestorage.StreamStorage, *subscribers.Subscribers, error) {
	storage, storageErr := filestorage.NewFileStorage(".hasty")
	if storageErr != nil {
		return master.MasterCommandHandler{}, filestorage.StreamStorage{}, nil, storageErr
	}

	streamStorage, streamStorageErr := filestorage.NewStreamStorage(storage)
	if streamStorageErr != nil {
		return master.MasterCommandHandler{}, filestorage.StreamStorage{}, nil, storageErr
	}

	subs := subscribers.NewSubscribers()

	master := master.NewMasterCommandHandler(&streamStorage, &subs)

	return master, streamStorage, &subs, nil
}

func (server *Server) Listen(host string, cert string, certPrivateKey string) error { // Listen for incoming connections.
	master, streamStorage, subs, _ := setupEnvironment()
	sub := subscriber.Subscriber{}
	commandHandler := commandhandler.NewCommandHandler(&sub, &master)

	log.Println("Listening to", host)
	config := new(tls.Config)
	certErr := setupCert(config, cert, certPrivateKey)
	if certErr != nil {
		log.Printf("Couldn't load certs '%s'", certErr)
		return certErr
	}
	listener, err := tls.Listen(CONN_TYPE, host, config)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return err
	}
	// Close the listener when the application closes.
	defer listener.Close()

	server.accepting(streamStorage, commandHandler, subs, listener)
	return nil
}

func (server *Server) accepting(storage filestorage.StreamStorage, handler handler.PacketHandler, subs *subscribers.Subscribers, listener net.Listener) {
	for {
		// Listen for an incoming connection.
		log.Printf("Waiting for accept...")
		conn, err := listener.Accept()
		if err != nil {
			log.Print("Error accepting: ", err)
		}
		server.nextConnectionIdentity++
		connectionIdentity := packet.NewConnectionID(server.nextConnectionIdentity)
		// Handle connections in a new goroutine.
		go handleRequest(storage, handler, subs, conn, connectionIdentity)
	}
}

// Handles incoming requests.
func handleRequest(storage filestorage.StreamStorage, mainHandler handler.PacketHandler, subs *subscribers.Subscribers, conn net.Conn, connectionIdentity packet.ConnectionID) {
	// Make a buffer to hold incoming data.
	// buf := make([]byte, 4096)
	defer conn.Close()
	log.Printf("Received a connection! '%s'", conn.RemoteAddr())
	temp := make([]byte, 1024)

	stream := packet.NewPacketStream(connectionIdentity)

	delegator := handler.NewPacketHandlerDelegator()
	delegator.AddHandler(mainHandler)
	connectionHandler := connection.NewConnectionHandler(&conn, &storage, subs, connectionIdentity)
	delegator.AddHandler(&connectionHandler)

	subs.Check()

	// l := log.New(os.Stderr, "", 0)

	for true {
		// Read the incoming connection into the buffer.
		n, err := conn.Read(temp)
		if err != nil {
			log.Printf("Closing Error reading: %s", err)
			return
		}
		data := temp[:n]

		// hexPayload := hex.Dump(data)
		// log.Printf("Received: %s", hexPayload)

		stream.Feed(data)
		newPacket, fetchErr := stream.FetchPacket()
		if fetchErr != nil {
			// log.Printf("Fetcherror:%s\n", fetchErr)
		} else {
			if newPacket.Payload() != nil {
				err := packetdeserializers.Deserialize(newPacket, &delegator)
				if err != nil {
					log.Printf("Deserialize error:%s", err)
				}
			}
		}
	}
}
