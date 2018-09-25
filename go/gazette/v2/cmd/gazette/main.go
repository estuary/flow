package main

import (
	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/broker"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	"github.com/LiveRamp/gazette/v2/pkg/http_gateway"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/metrics"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
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

	var etcd = mbp.MustEtcdContext(Config.Etcd.EtcdConfig)
	var srv = mbp.MustBuildServer(Config.Broker.ServiceConfig)
	protocol.RegisterGRPCDispatcher(Config.Broker.Zone)

	var lo = protocol.NewJournalClient(srv.Loopback())
	var service = broker.NewService(allocState, lo, etcd.Etcd)
	var rjc = protocol.NewRoutedJournalClient(lo, service)

	protocol.RegisterJournalServer(srv.GRPCServer, service)
	srv.HTTPMux.Handle("/", http_gateway.NewGateway(rjc))

	var persister = fragment.NewPersister()
	go persister.Serve()
	broker.SetSharedPersister(persister)

	mbp.AnnounceServeAndAllocate(etcd, srv, allocState, &protocol.BrokerSpec{
		ProcessSpec:  Config.Broker.ProcessSpec(),
		JournalLimit: Config.Broker.Limit,
	})

	persister.Finish()
	log.Info("goodbye")
	return nil
}

func main() {
	var parser = flags.NewParser(Config, flags.Default)

	parser.AddCommand("serve", "Serve as Gazette broker", `
serve a Gazette broker with the provided configuration, until signaled to
exit (via SIGTERM). Upon receiving a signal, the broker will seek to discharge
its responsible journals and will exit only when it can safely do so.
`, &serveBroker{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
