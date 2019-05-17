package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/broker"
	"go.gazette.dev/core/fragment"
	"go.gazette.dev/core/http_gateway"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/metrics"
	"go.gazette.dev/core/protocol"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

const iniFilename = "gazette.ini"

// Config is the top-level configuration object of a Gazette broker.
var Config = new(struct {
	Broker struct {
		mbp.ServiceConfig
		Limit uint32 `long:"limit" env:"LIMIT" default:"1024" description:"Maximum number of Journals the broker will allocate"`
	} `group:"Broker" namespace:"broker" env-namespace:"BROKER"`

	Etcd struct {
		mbp.EtcdConfig
		Prefix string `long:"prefix" env:"PREFIX" default:"/gazette/brokers" description:"Etcd base prefix for broker state and coordination"`
	} `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`

	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
})

type serveBroker struct{}

func (serveBroker) Execute(args []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)

	log.WithField("config", Config).Info("starting broker")
	prometheus.MustRegister(metrics.GazetteBrokerCollectors()...)

	var ks = broker.NewKeySpace(Config.Etcd.Prefix)
	var allocState = allocator.NewObservedState(ks, Config.Broker.MemberKey(ks))

	var etcd = Config.Etcd.MustDial()
	var srv, err = server.New("", Config.Broker.Port)
	mbp.Must(err, "building Server instance")
	protocol.RegisterGRPCDispatcher(Config.Broker.Zone)

	var lo = protocol.NewJournalClient(srv.MustGRPCLoopback())
	var service = broker.NewService(allocState, lo, etcd)
	var rjc = protocol.NewRoutedJournalClient(lo, service)

	protocol.RegisterJournalServer(srv.GRPCServer, service)
	srv.HTTPMux.Handle("/", http_gateway.NewGateway(rjc))

	var tasks = task.NewGroup(context.Background())
	srv.QueueTasks(tasks)

	var persister = fragment.NewPersister(ks)
	broker.SetSharedPersister(persister)

	tasks.Queue("persister.Serve", func() error {
		persister.Serve()
		return nil
	})

	var signalCh = make(chan os.Signal, 1)

	mbp.Must(allocator.StartSession(allocator.SessionArgs{
		Etcd:     etcd,
		LeaseTTL: Config.Etcd.LeaseTTL,
		SignalCh: signalCh,
		Spec: &protocol.BrokerSpec{
			JournalLimit: Config.Broker.Limit,
			ProcessSpec:  Config.Broker.ProcessSpec(),
		},
		State: allocState,
		Tasks: tasks,
	}), "starting allocator session")

	tasks.Queue("service.Watch", func() error {
		var err = service.Watch(tasks.Context())
		// At Watch return, we're assured that all journal replicas have been
		// fully shut down. Ask the persister to Finish persisting any
		// outstanding local spools.
		persister.Finish()
		return err
	})

	// Install signal handler & start broker tasks.
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
	tasks.GoRun()

	// Block until all tasks complete. Assert none returned an error.
	mbp.Must(tasks.Wait(), "broker task failed")
	log.Info("goodbye")

	return nil
}

func main() {
	var parser = flags.NewParser(Config, flags.Default)

	_, _ = parser.AddCommand("serve", "Serve as Gazette broker", `
Serve a Gazette broker with the provided configuration, until signaled to
exit (via SIGTERM). Upon receiving a signal, the broker will seek to discharge
its responsible journals and will exit only when it can safely do so.
`, &serveBroker{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
