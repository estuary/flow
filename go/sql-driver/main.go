package main

import (
	"fmt"
	"net"
	"os"

	"github.com/estuary/flow/go/materialize/driver/sql"
	"github.com/estuary/flow/go/protocols/materialize"
	flags "github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type positional struct {
	Driver string `required:"true" choice:"postgres" choice:"sqlite"`
}

type args struct {
	Listen     string     `long:"listen" optional:"true" default:"127.0.0.1:9191" description:"The host and port number to bind to. Leave the host blank to listen on all interfaces."`
	Positional positional `positional-args:"yuup"`
}

func main() {
	var opts args
	var parser = flags.NewParser(&opts, flags.Default)
	log.SetLevel(log.DebugLevel)

	if _, err := parser.Parse(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var server materialize.DriverServer
	switch opts.Positional.Driver {
	case "sqlite":
		server = sql.NewSQLiteDriver()
	default:
		fmt.Printf("Invalid driver argument: '%s'", opts.Positional.Driver)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		os.Exit(1)
	}
	var grpcServer = grpc.NewServer()
	materialize.RegisterDriverServer(grpcServer, server)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to server: %v", err)
		os.Exit(1)
	}
}
