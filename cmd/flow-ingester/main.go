package main

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/ingest"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

const iniFilename = "flow.ini"

// Config is the top-level configuration object of a Flow ingester.
var Config = new(struct {
	Ingest struct {
		mbp.ServiceConfig
		Catalog string `long:"catalog" required:"true" description:"Catalog URL or local path"`
	} `group:"Ingest" namespace:"ingest" env-namespace:"INGEST"`

	Flow struct {
		BrokerRoot string `long:"broker-root" env:"BROKER_ROOT" default:"/gazette/cluster" description:"Broker Etcd base prefix"`
	} `group:"flow" namespace:"flow" env-namespace:"FLOW"`

	Etcd        mbp.EtcdConfig        `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
})

type ingesterTesting flow.Ingester

// AdvanceTime advances the current test time.
func (i *ingesterTesting) AdvanceTime(_ context.Context, req *pf.AdvanceTimeRequest) (*pf.AdvanceTimeResponse, error) {
	var add = uint64(time.Second) * req.AddClockDeltaSeconds
	var out = time.Duration(atomic.AddInt64((*int64)(&i.PublishClockDelta), int64(add)))

	// Block until a current transaction (if any) commits, ensuring
	// the next transaction will see and apply the updated time delta.
	var ingest = (*flow.Ingester)(i).Start()
	var _, err = ingest.PrepareAndAwait()

	return &pf.AdvanceTimeResponse{ClockDeltaSeconds: uint64(out / time.Second)}, err
}

type cmdServe struct{}

func (cmdServe) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("ingester configuration")
	pb.RegisterGRPCDispatcher(Config.Ingest.Zone)

	catalog, err := flow.NewCatalog(Config.Ingest.Catalog, os.TempDir())
	mbp.Must(err, "opening catalog")
	collections, err := catalog.LoadCapturedCollections()
	mbp.Must(err, "loading captured collection specifications")

	for _, collection := range collections {
		log.WithField("name", collection.Name).Info("serving captured collection")
	}
	// Bind our server listener, grabbing a random available port if Port is zero.
	srv, err := server.New("", Config.Ingest.Port)
	mbp.Must(err, "building Server instance")

	if Config.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}

	var (
		etcd     = Config.Etcd.MustDial()
		spec     = Config.Ingest.BuildProcessSpec(srv)
		rjc      = Config.Broker.MustRoutedJournalClient(context.Background())
		tasks    = task.NewGroup(context.Background())
		signalCh = make(chan os.Signal, 1)
	)

	journals, err := flow.NewJournalsKeySpace(context.Background(), etcd, Config.Flow.BrokerRoot)
	mbp.Must(err, "failed to load Gazette journals")
	delegate, err := flow.NewWorkerHost("combine", "--catalog", catalog.LocalPath())
	mbp.Must(err, "failed to start flow-worker")

	var ingester = &flow.Ingester{
		Collections: collections,
		Combiner:    pf.NewCombineClient(delegate.Conn),
		Mapper: &flow.Mapper{
			Ctx:           tasks.Context(),
			JournalClient: rjc,
			Journals:      journals,
		},
	}
	ingester.QueueTasks(tasks, rjc)

	ingest.RegisterAPIs(srv, ingester, journals)

	pf.RegisterTestingServer(srv.GRPCServer, (*ingesterTesting)(ingester))
	srv.QueueTasks(tasks)

	tasks.Queue("journals.Watch", func() error {
		if err := journals.Watch(tasks.Context(), etcd); err != context.Canceled {
			return err
		}
		return nil
	})

	log.WithFields(log.Fields{
		"zone":     spec.Id.Zone,
		"id":       spec.Id.Suffix,
		"endpoint": spec.Endpoint,
	}).Info("starting flow-ingester")

	// Install signal handler & start broker tasks.
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	tasks.Queue("watch signalCh", func() error {
		select {
		case sig := <-signalCh:
			log.WithField("signal", sig).Info("caught signal")

			tasks.Cancel()
			srv.BoundedGracefulStop()
			return delegate.Stop()

		case <-tasks.Context().Done():
			return nil
		}
	})
	tasks.GoRun()

	// Block until all tasks complete. Assert none returned an error.
	mbp.Must(tasks.Wait(), "ingester task failed")
	log.Info("goodbye")

	return nil
}

func main() {
	var parser = flags.NewParser(Config, flags.Default)

	_, _ = parser.AddCommand("serve", "Serve as Flow ingester", `
Serve a Flow ingester with the provided configuration, until signaled to
exit (via SIGTERM).
`, &cmdServe{})

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}
