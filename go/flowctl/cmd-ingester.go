package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/estuary/flow/go/runtime"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	server "go.gazette.dev/core/server"
	"go.gazette.dev/core/task"
)

type cmdIngester struct {
	runtime.FlowIngesterConfig
}

func (cmd cmdIngester) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	pb.RegisterGRPCDispatcher(cmd.Ingest.Zone)

	if cmd.Broker.Cache.Size <= 0 {
		log.Warn("--broker.cache.size is disabled; consider setting > 0")
	}

	// Bind our server listener, grabbing a random available port if Port is zero.
	var server, err = server.New("", cmd.Ingest.Port)
	if err != nil {
		return fmt.Errorf("building server: %w", err)
	}

	var args = runtime.FlowIngesterArgs{
		BrokerRoot:  cmd.Flow.BrokerRoot,
		CatalogRoot: cmd.Flow.CatalogRoot,
		Server:      server,
		Tasks:       task.NewGroup(context.Background()),
		Journals:    cmd.Broker.MustRoutedJournalClient(context.Background()),
		Etcd:        cmd.Etcd.MustDial(),
	}
	if _, err = runtime.StartIngesterService(args); err != nil {
		return fmt.Errorf("starting ingester service: %w", err)
	}
	args.Server.QueueTasks(args.Tasks)

	log.WithFields(log.Fields{
		"zone":     cmd.Ingest.Zone,
		"endpoint": cmd.Ingest.BuildProcessSpec(server).Endpoint,
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
