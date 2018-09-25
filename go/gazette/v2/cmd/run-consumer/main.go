package main

import (
	"context"
	"fmt"
	"os"
	"plugin"

	"github.com/LiveRamp/gazette/v2/cmd/run-consumer/consumermodule"
	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/LiveRamp/gazette/v2/pkg/metrics"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const iniFilename = "gazette.ini"

var (
	Module consumermodule.Module
	Config consumermodule.Config
)

type serveConsumer struct{}

func (serveConsumer) Execute(args []string) error {
	var cfg = Config.GetBaseConfig()

	defer mbp.InitDiagnosticsAndRecover(cfg.Diagnostics)()
	mbp.InitLog(cfg.Log)

	log.WithField("config", Config).Info("starting consumer")
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)
	prometheus.MustRegister(metrics.GazetteConsumerCollectors()...)

	var ks = consumer.NewKeySpace(cfg.Etcd.Prefix)
	var allocState = allocator.NewObservedState(ks, cfg.Consumer.MemberKey(ks))

	var etcd = mbp.MustEtcdContext(cfg.Etcd.EtcdConfig)
	var app = Module.NewApplication(Config)
	var srv = mbp.MustBuildServer(cfg.Consumer.ServiceConfig)
	protocol.RegisterGRPCDispatcher(cfg.Consumer.Zone)

	if cfg.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}
	var rjc = cfg.Broker.RoutedJournalClient(context.Background())
	var service = consumer.NewService(app, allocState, rjc, srv.Loopback(), etcd.Etcd)

	consumer.RegisterShardServer(srv.GRPCServer, service)
	Module.Register(Config, app, srv, service)

	mbp.AnnounceServeAndAllocate(etcd, srv, allocState, &consumer.ConsumerSpec{
		ProcessSpec: cfg.Consumer.ProcessSpec(),
		ShardLimit:  cfg.Consumer.Limit,
	})

	log.Info("goodbye")
	return nil
}

func main() {
	Module = bootstrapModule()
	Config = Module.NewConfig()

	var parser = flags.NewParser(Config, flags.Default)
	parser.AddCommand("serve", "Serve as Gazette consumer", `
		serve a Gazette consumer with the provided configuration, until signaled to
		exit (via SIGTERM). Upon receiving a signal, the consumer will seek to discharge
		its responsible shards and will exit only when it can safely do so.
		`, &serveConsumer{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}

func bootstrapModule() consumermodule.Module {
	var cfg struct {
		Consumer struct {
			Module string `long:"module" env:"MODULE" description:"Path to consumer module to dynamically load"`
		} `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	}

	var parser = flags.NewParser(&cfg, flags.PrintErrors|flags.IgnoreUnknown)
	mbp.MustParseArgs(parser)

	if cfg.Consumer.Module == "" {
		parser.WriteHelp(os.Stderr)
	}
	if m, err := plugin.Open(cfg.Consumer.Module); err != nil {
		log.WithFields(log.Fields{
			"module": cfg.Consumer.Module,
			"err":    err,
		}).Fatal("failed to open module")
	} else if i, err := m.Lookup("Module"); err != nil {
		log.WithFields(log.Fields{
			"module": cfg.Consumer.Module,
			"err":    err,
		}).Fatal("failed to lookup `Module` symbol")
	} else if ptr, ok := i.(*consumermodule.Module); !ok {
		log.WithFields(log.Fields{
			"module":   cfg.Consumer.Module,
			"instance": fmt.Sprintf("%#v", i),
		}).Fatal("Module instance is not a consumermodule.Module")
	} else {
		return *ptr
	}
	panic("not reached")
}
