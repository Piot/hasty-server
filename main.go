package main

import (
	"log"

	"github.com/fatih/color"
	"github.com/piot/hasty-server/config"
	"github.com/piot/hasty-server/server"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	verbose = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
	host    = kingpin.Flag("listen", "Which port (and address) to bind to").Short('l').Default("0.0.0.0:3333").String()
	cert    = kingpin.Flag("cert", "SSL cert to use (PEM encoded)").Default(".config/certs/cert.pem").String()
	key     = kingpin.Flag("key", "SSL Certificate Private Key (PEM encoded)").Default(".config/certs/key.pem").String()
)

func main() {
	color.Cyan("Hastyd v0.1.1")
	kingpin.Parse()
	s := hastyserver.NewServer()
	config, err := config.LoadConfig()
	if err != nil {
		return
	}
	log.Printf("config url:%v", config.Authentication.URL)
	listenErr := s.Listen(*host, *cert, *key, config)
	if listenErr != nil {
		log.Printf("Error:%s", listenErr)
	}
}
