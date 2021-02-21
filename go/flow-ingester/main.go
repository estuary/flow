package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/runtime"
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	server "go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

const iniFilename = "flow.ini"

// Config is the top-level configuration object of a Flow ingester.
var Config = new(runtime.FlowIngesterConfig)

type cmdServe struct{}

func (cmdServe) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flow-ingester configuration")

	pb.RegisterGRPCDispatcher(Config.Ingest.Zone)

	if Config.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}

	// Bind our server listener, grabbing a random available port if Port is zero.
	var server, err = server.New("", Config.Ingest.Port)
	if err != nil {
		return fmt.Errorf("building server: %w", err)
	}

	catalog, err := flow.NewCatalog(Config.Ingest.Catalog, os.TempDir())
	if err != nil {
		return fmt.Errorf("opening catalog: %w", err)
	}

	var args = runtime.FlowIngesterArgs{
		Catalog:    catalog,
		BrokerRoot: Config.Flow.BrokerRoot,
		Server:     server,
		Tasks:      task.NewGroup(context.Background()),
		Journals:   Config.Broker.MustRoutedJournalClient(context.Background()),
		Etcd:       Config.Etcd.MustDial(),
	}
	if _, err = runtime.StartIngesterService(args); err != nil {
		return fmt.Errorf("starting ingester service: %w", err)
	}
	args.Server.QueueTasks(args.Tasks)

	log.WithFields(log.Fields{
		"zone":     Config.Ingest.Zone,
		"endpoint": Config.Ingest.BuildProcessSpec(server).Endpoint,
	}).Info("starting flow-ingester")

	// Install signal handler & start broker tasks.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	args.Tasks.Queue("watch signalCh", func() error {
		select {
		case sig := <-signalCh:
			log.WithField("signal", sig).Info("caught signal")

			args.Tasks.Cancel()
			server.BoundedGracefulStop()
			return nil

		case <-args.Tasks.Context().Done():
			return nil
		}
	})
	args.Tasks.GoRun()

	// Block until all tasks complete.
	if err = args.Tasks.Wait(); err != nil {
		return fmt.Errorf("task failed: %w", err)
	}

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
