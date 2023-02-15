package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdTest struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
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
	}).Info("flowctl configuration")
	protocol.RegisterGRPCDispatcher("local")

	var err error
	if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("filepath.Abs: %w", err)
	}

	// Create a temporary directory which will contain the Etcd database
	// and various unix:// sockets.
	tempdir, err := ioutil.TempDir("", "flow-test")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tempdir)

	// Install a signal handler which will cancel our context.
	var ctx, cancel = signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Start a temporary data plane bound to our context.
	var dataPlane = cmdTempDataPlane{
		UnixSockets: true,
		Log: mbp.LogConfig{
			Level:  "warn",
			Format: cmd.Log.Format,
		},
	}
	_, brokerAddr, consumerAddr, err := dataPlane.start(ctx, tempdir)
	if err != nil {
		return fmt.Errorf("starting local data plane: %w", err)
	}

	// Build into a new database. Arrange to clean it up on exit.
	var buildID = newBuildID()
	defer func() { _ = os.Remove(filepath.Join(cmd.Directory, buildID)) }()

	if err := (apiBuild{
		BuildID:    buildID,
		Directory:  cmd.Directory,
		FileRoot:   "/",
		Network:    cmd.Network,
		Dataplane:  "local",
		Source:     cmd.Source,
		SourceType: "catalog",
		TSPackage:  true,
	}.execute(ctx)); err != nil {
		return err
	}

	// Move the build database into the data plane temp directory.
	// Shell to `mv` (vs os.Rename) for it's proper handling of cross-volume moves.
	if err := exec.Command("mv",
		filepath.Join(cmd.Directory, buildID),
		filepath.Join(tempdir, "builds", buildID),
	).Run(); err != nil {
		return fmt.Errorf("moving build to local data plane builds root: %w", err)
	}

	// Activate derivations of the built database into the local dataplane.
	var activate = apiActivate{
		BuildID:        buildID,
		Network:        cmd.Network,
		InitialSplits:  1,
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

	if err = test.execute(ctx); err != nil {
		return err
	}

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

	return nil
}
