package main

import (
	"context"
	"fmt"
	//"net"
	"os"
	"os/signal"
	//"sync/atomic"
	"syscall"

	"github.com/estuary/flow/go/materialize/driver/sql"
	"github.com/estuary/flow/go/protocols/materialize"
	flags "github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
	//"google.golang.org/grpc"
)

type positional struct {
	Driver string `required:"true" choice:"postgres" choice:"sqlite"`
}

type listen struct {
	Port      uint16 `long:"port" optional:"true" default:"9191" description:"The port number to bind to"`
	Interface string `long:"interface" optional:"true" default:""`
}

type args struct {
	Listen      listen                `group:"Listen" namespace:"listen" env-namespace:"LISTEN"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Positional  positional            `positional-args:"yuup"`
}

func main() {
	var opts args
	var parser = flags.NewParser(&opts, flags.Default)

	if _, err := parser.Parse(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer mbp.InitDiagnosticsAndRecover(opts.Diagnostics)()
	mbp.InitLog(opts.Log)
	pb.RegisterGRPCDispatcher("local")

	var driverServer materialize.DriverServer
	switch opts.Positional.Driver {
	case "sqlite":
		driverServer = sql.NewSQLiteDriver()
	case "postgres":
		driverServer = sql.NewPostgresDriver()
	default:
		fmt.Printf("Invalid driver argument: '%s'", opts.Positional.Driver)
		os.Exit(1)
	}

	srv, err := server.New(opts.Listen.Interface, opts.Listen.Port)
	mbp.Must(err, "building Server instance")

	//var grpcServer = grpc.NewServer()
	materialize.RegisterDriverServer(srv.GRPCServer, driverServer)

	var tasks = task.NewGroup(context.Background())
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	tasks.Queue("watch signalCh", func() error {
		select {
		case sig := <-signalCh:
			log.WithField("signal", sig).Info("caught signal")

			tasks.Cancel()
			srv.BoundedGracefulStop()
			return nil

		case <-tasks.Context().Done():
			return nil
		}
	})
	srv.QueueTasks(tasks)
	tasks.GoRun()

	// Block until all tasks complete. Assert none returned an error.
	mbp.Must(tasks.Wait(), "ingester task failed")
	log.Info("goodbye")
}
