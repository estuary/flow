// Package runconsumer extends consumer.Application with support for
// configuration and application initialization. It provides a Main function
// which executes the full consumer life-cycle, including config parsing,
// service bootstrap, and Shard serving.
package runconsumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// Application is the user-defined consumer Application which is executed
// by Main. It extends consumer.Application with callbacks to support
// custom configuration parsing and initialization.
type Application interface {
	consumer.Application

	// NewConfig returns a new, zero-valued Config instance.
	// Main calls NewConfig to obtain a new instance of the Application's
	// custom configuration type. It will next use `go-flags` to parse
	// command-line and environment flags into the provide Config, in order
	// to provide the Application with a complete configuration.
	NewConfig() Config
	// InitApplication initializes the Application.
	// Main calls InitApplication after parsing the Config and binding
	// HTTP and gRPC servers, but before announcing this process's
	// MemberSpec.
	//
	// InitApplication is a good opportunity to register additional gRPC
	// services or perform other initialization.
	InitApplication(InitArgs) error
}

// InitArgs are arguments passed to Application.InitApplication.
type InitArgs struct {
	// Context of the service. Typically this is context.Background(),
	// but tests may prefer to use a scoped context.
	Context context.Context
	// Config previously returned by NewConfig, and since parsed into.
	Config Config
	// Server is a dual HTTP and gRPC Server. Applications may register
	// APIs they implement against the Server mux.
	Server *server.Server
	// Service of the consumer. Applications may use the Service to power Shard
	// resolution, request proxying, and state inspection.
	Service *consumer.Service
	// Tasks are independent, cancelable goroutines having the lifetime of
	// the consumer, such as service loops and the like. Applications may
	// add additional tasks which should be started with the consumer.
	Tasks *task.Group
}

// Config is the top-level configuration object of an Application. It must
// be parse-able by `go-flags`, and must present a BaseConfig.
type Config interface {
	GetBaseConfig() BaseConfig
}

// BaseConfig is the top-level configuration object of a Gazette consumer.
type BaseConfig struct {
	Consumer struct {
		mbp.ServiceConfig
		Limit uint32 `long:"limit" env:"LIMIT" default:"32" description:"Maximum number of Shards this consumer process will allocate"`
	} `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`

	Broker struct {
		mbp.ClientConfig
		FileRoot string `long:"file-root" env:"FILE_ROOT" description:"Local path which roots file:// fragment URLs which are being directly read (optional)"`
	} `group:"Broker" namespace:"broker" env-namespace:"BROKER"`

	Etcd struct {
		mbp.EtcdConfig

		Prefix string `long:"prefix" env:"PREFIX" default-mask:"/gazette/consumers/app-name-and-release" description:"Etcd prefix for the consumer group"`
	} `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`

	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

// GetBaseConfig returns itself, and trivially implements the Config interface.
func (c BaseConfig) GetBaseConfig() BaseConfig { return c }

const iniFilename = "gazette.ini"

// Cmd wraps a Config and Application to provide an Execute entry-point.
type Cmd struct {
	Cfg Config
	App Application
}

func (sc Cmd) Execute(args []string) error {
	var bc = sc.Cfg.GetBaseConfig()

	defer mbp.InitDiagnosticsAndRecover(bc.Diagnostics)()
	mbp.InitLog(bc.Log)

	log.WithFields(log.Fields{
		"config":    sc.Cfg,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("consumer configuration")
	pb.RegisterGRPCDispatcher(bc.Consumer.Zone)

	// Bind our server listener, grabbing a random available port if Port is zero.
	var srv, err = server.New("", bc.Consumer.Port)
	mbp.Must(err, "building Server instance")

	if bc.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}
	// If a file:// root was provided, ensure it exists and install it as a transport.
	if bc.Broker.FileRoot != "" {
		_, err = os.Stat(bc.Broker.FileRoot)
		mbp.Must(err, "configured local file:// root failed")
		defer client.InstallFileTransport(bc.Broker.FileRoot)()
	}
	// If an Etcd prefix isn't provided, synthesize one using the application type.
	if bc.Etcd.Prefix == "" {
		bc.Etcd.Prefix = fmt.Sprintf("/gazette/consumers/%T", sc.App)
	}

	var (
		etcd = bc.Etcd.MustDial()
		spec = &pc.ConsumerSpec{
			ShardLimit:  bc.Consumer.Limit,
			ProcessSpec: bc.Consumer.BuildProcessSpec(srv),
		}
		ks       = consumer.NewKeySpace(bc.Etcd.Prefix)
		state    = allocator.NewObservedState(ks, allocator.MemberKey(ks, spec.Id.Zone, spec.Id.Suffix), consumer.ShardIsConsistent)
		rjc      = bc.Broker.MustRoutedJournalClient(context.Background())
		service  = consumer.NewService(sc.App, state, rjc, srv.GRPCLoopback, etcd)
		tasks    = task.NewGroup(context.Background())
		signalCh = make(chan os.Signal, 1)
	)
	pc.RegisterShardServer(srv.GRPCServer, service)

	// Register Resolver as a prometheus.Collector for tracking shard status
	prometheus.MustRegister(service.Resolver)

	log.WithFields(log.Fields{
		"zone":     spec.Id.Zone,
		"id":       spec.Id.Suffix,
		"endpoint": spec.Endpoint,
		"group":    bc.Etcd.Prefix,
	}).Info("starting consumer")

	mbp.Must(sc.App.InitApplication(InitArgs{
		Context: context.Background(),
		Config:  sc.Cfg,
		Server:  srv,
		Service: service,
		Tasks:   tasks,
	}), "failed to init application")

	mbp.Must(allocator.StartSession(allocator.SessionArgs{
		Etcd:     etcd,
		LeaseTTL: bc.Etcd.LeaseTTL,
		SignalCh: signalCh,
		Spec:     spec,
		State:    state,
		Tasks:    tasks,
	}), "failed to start allocator session")

	srv.QueueTasks(tasks)
	service.QueueTasks(tasks, srv)

	// Install signal handler, and launch consumer tasks.
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
	tasks.GoRun()

	// Block until all tasks complete. Assert none returned an error.
	mbp.Must(tasks.Wait(), "consumer task failed")
	log.Info("goodbye")

	return nil
}

func Main(app Application) {
	var cfg = app.NewConfig()

	var parser = flags.NewParser(cfg, flags.Default)
	_, _ = parser.AddCommand("serve", "Serve as Gazette consumer", `
		serve a Gazette consumer with the provided configuration, until signaled to
		exit (via SIGTERM). Upon receiving a signal, the consumer will seek to discharge
		its responsible shards and will exit only when it can safely do so.
		`, &Cmd{Cfg: cfg, App: app})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
