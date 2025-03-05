package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdTest struct {
	Network     string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Snapshot    string                `long:"snapshot" description:"When set, failed test verifications produce snapshots into the given base directory"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdTest) Execute(_ []string) (retErr error) {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Debug("flowctl configuration")
	protocol.RegisterGRPCDispatcher("local")

	// Create a temporary directory which will contain the Etcd database
	// and various unix:// sockets.
	tempdir, err := os.MkdirTemp("", "flow-test")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tempdir)

	// Install a signal handler which will cancel our context.
	var ctx, cancel = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Start a temporary data plane bound to our context.
	var dataPlane = cmdTempDataPlane{
		UnixSockets:  true,
		BrokerPort:   8080,
		ConsumerPort: 9000,
		Log: mbp.LogConfig{
			Level:  cmd.Log.Level,
			Format: cmd.Log.Format,
		},
	}
	_, brokerAddr, consumerAddr, err := dataPlane.start(ctx, tempdir)
	if err != nil {
		return fmt.Errorf("starting local data plane: %w", err)
	}

	var buildID = "ffffffffffffffff"

	if err := (apiBuild{
		BuildID: buildID,
		// Build directly into the temp dataplane's build directory.
		BuildDB:    filepath.Join(tempdir, "builds", buildID),
		FileRoot:   "/",
		Network:    cmd.Network,
		Source:     cmd.Source,
		SourceType: "catalog",
	}.execute(ctx)); err != nil {
		return err
	}

	// Activate derivations of the built database into the local dataplane.
	var activate = apiActivate{
		BuildID:        buildID,
		Network:        cmd.Network,
		InitialSplits:  3,
		AllDerivations: true,
	}
	activate.Broker.Address = protocol.Endpoint(brokerAddr)
	activate.Consumer.Address = protocol.Endpoint(consumerAddr)

	if err = activate.execute(ctx); err != nil && err != errNoChangesToApply {
		return err
	}

	// Test the built database against the local dataplane.
	var test = apiTest{
		BuildID:  buildID,
		Snapshot: cmd.Snapshot,
	}
	test.Broker.Address = protocol.Endpoint(brokerAddr)
	test.Consumer.Address = protocol.Endpoint(consumerAddr)

	var testErr = test.execute(ctx)

	// Delete derivations and collections from the local dataplane.
	var delete = apiDelete{
		BuildID:        buildID,
		Network:        cmd.Network,
		AllDerivations: true,
	}
	delete.Broker.Address = protocol.Endpoint(brokerAddr)
	delete.Consumer.Address = protocol.Endpoint(consumerAddr)

	if err = delete.execute(ctx); err != nil && err != errNoChangesToApply {
		return err
	}

	// Stop the data plane. It exits as we've removed all entities.
	dataPlane.gracefulStop()

	return testErr
}
