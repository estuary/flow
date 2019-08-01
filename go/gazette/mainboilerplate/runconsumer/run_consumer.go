// Package runconsumer extends consumer.Application with support for
// configuration and application initialization. It provides a Main() function
// which executes the full consumer life-cycle, including config parsing,
// service bootstrap, and Shard serving.
package runconsumer

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/metrics"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

// Application interface run by Run.
type Application interface {
	consumer.Application

	// NewConfig returns a new, zero-valued Config instance.
	NewConfig() Config
	// InitApplication initializes the Application, eg by starting related
	// services and registering implemented service APIs.
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
	// GetBaseConfig of the Config.
	GetBaseConfig() BaseConfig
}

// BaseConfig is the top-level configuration object of a Gazette consumer.
type BaseConfig struct {
	Consumer struct {
		mbp.ServiceConfig

		Limit uint32 `long:"limit" env:"LIMIT" default:"32" description:"Maximum number of Shards this consumer process will allocate"`
	} `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`

	Broker mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`

	Etcd struct {
		mbp.EtcdConfig

		Prefix string `long:"prefix" env:"PREFIX" description:"Etcd prefix for consumer state and coordination (eg, /gazette/consumers/myApplication)"`
	} `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`

	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

// GetBaseConfig returns itself, and trivially implements the Config interface.
func (c BaseConfig) GetBaseConfig() BaseConfig { return c }

const iniFilename = "gazette.ini"

type serveConsumer struct {
	cfg Config
	app Application
}

func (sc serveConsumer) Execute(args []string) error {
	var bc = sc.cfg.GetBaseConfig()

	defer mbp.InitDiagnosticsAndRecover(bc.Diagnostics)()
	mbp.InitLog(bc.Log)

	log.WithFields(log.Fields{
		"config":    sc.cfg,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("starting consumer")
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)
	prometheus.MustRegister(metrics.GazetteConsumerCollectors()...)
	pb.RegisterGRPCDispatcher(bc.Consumer.Zone)

	var ks = consumer.NewKeySpace(bc.Etcd.Prefix)
	var allocState = allocator.NewObservedState(ks, bc.Consumer.MemberKey(ks))

	var etcd = bc.Etcd.MustDial()
	var srv, err = server.New("", bc.Consumer.Port)
	mbp.Must(err, "building Server instance")

	if bc.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}
	var rjc = bc.Broker.MustRoutedJournalClient(context.Background())
	var service = consumer.NewService(sc.app, allocState, rjc, srv.GRPCLoopback, etcd)

	var tasks = task.NewGroup(context.Background())

	pc.RegisterShardServer(srv.GRPCServer, service)
	mbp.Must(sc.app.InitApplication(InitArgs{
		Context: context.Background(),
		Config:  sc.cfg,
		Server:  srv,
		Service: service,
		Tasks:   tasks,
	}), "application failed to init")

	var signalCh = make(chan os.Signal, 1)

	mbp.Must(allocator.StartSession(allocator.SessionArgs{
		Etcd:     etcd,
		LeaseTTL: bc.Etcd.LeaseTTL,
		SignalCh: signalCh,
		Spec: &pc.ConsumerSpec{
			ProcessSpec: bc.Consumer.ProcessSpec(),
			ShardLimit:  bc.Consumer.Limit,
		},
		State: allocState,
		Tasks: tasks,
	}), "starting allocator session")

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
		`, &serveConsumer{cfg: cfg, app: app})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
