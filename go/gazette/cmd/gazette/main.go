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
	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/fragment"
	"go.gazette.dev/core/http_gateway"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/metrics"
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

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("starting broker")
	prometheus.MustRegister(metrics.GazetteBrokerCollectors()...)
	protocol.RegisterGRPCDispatcher(Config.Broker.Zone)

	var ks = broker.NewKeySpace(Config.Etcd.Prefix)
	var allocState = allocator.NewObservedState(ks, Config.Broker.MemberKey(ks))

	var etcd = Config.Etcd.MustDial()
	var srv, err = server.New("", Config.Broker.Port)
	mbp.Must(err, "building Server instance")

	var lo = protocol.NewJournalClient(srv.GRPCLoopback)
	var service = broker.NewService(allocState, lo, etcd)
	var rjc = protocol.NewRoutedJournalClient(lo, service)

	protocol.RegisterJournalServer(srv.GRPCServer, service)
	srv.HTTPMux.Handle("/", http_gateway.NewGateway(rjc))

	var tasks = task.NewGroup(context.Background())
	var signalCh = make(chan os.Signal, 1)

	mbp.Must(allocator.StartSession(allocator.SessionArgs{
		Etcd:  etcd,
		Tasks: tasks,
		Spec: &protocol.BrokerSpec{
			JournalLimit: Config.Broker.Limit,
			ProcessSpec:  Config.Broker.ProcessSpec(),
		},
		State:    allocState,
		LeaseTTL: Config.Etcd.LeaseTTL,
		SignalCh: signalCh,
	}), "starting allocator session")

	var persister = fragment.NewPersister(ks)
	broker.SetSharedPersister(persister)

	tasks.Queue("persister.Serve", func() error {
		persister.Serve()
		return nil
	})

	srv.QueueTasks(tasks)
	service.QueueTasks(tasks, srv, persister.Finish)

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
