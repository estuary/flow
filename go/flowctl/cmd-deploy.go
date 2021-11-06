package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdDeploy struct {
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Network     string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Cleanup     bool                  `long:"wait-and-cleanup" description:"Keep running after deploy until Ctrl-C. Then, delete the deployment from the dataplane."`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdDeploy) Execute(_ []string) (retErr error) {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	var err error
	if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("filepath.Abs: %w", err)
	}

	// Build into a new database. Arrange to clean it up on exit if asked.
	var buildID = newBuildID()
	if cmd.Cleanup {
		defer func() { _ = os.Remove(filepath.Join(cmd.Directory, buildID)) }()
	}

	if err := (apiBuild{
		BuildID:    buildID,
		Directory:  cmd.Directory,
		FileRoot:   "/",
		Network:    cmd.Network,
		Source:     cmd.Source,
		SourceType: "catalog",
		TSPackage:  true,
	}.execute(context.Background())); err != nil {
		return err
	}

	// TODO(johnny): Ask the data plane, rather than assuming cmd.Directory.
	var buildsRoot = "file://" + cmd.Directory + "/"

	// TODO(johnny): Move into the data plane's advertised buildsRoot.

	// Activate the built database into the data plane.
	var activate = apiActivate{
		Broker:        cmd.Broker,
		Consumer:      cmd.Consumer,
		BuildID:       buildID,
		BuildsRoot:    buildsRoot,
		Network:       cmd.Network,
		InitialSplits: 1,
		All:           true,
	}
	if err = activate.execute(context.Background()); err != nil {
		return err
	}

	if !cmd.Cleanup {
		return nil // All done.
	}

	// Install a signal handler which will cancel our context.
	var sigCh = make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	fmt.Println("Deployment done. Waiting for Ctrl-C to clean up and exit.")
	<-sigCh
	fmt.Println("Signaled to exit. Cleaning up deployment.")

	// Delete derivations and collections from the local dataplane.
	var delete = apiDelete{
		Broker:     cmd.Broker,
		Consumer:   cmd.Consumer,
		BuildID:    buildID,
		BuildsRoot: buildsRoot,
		Network:    cmd.Network,
		All:        true,
	}
	if err = delete.execute(context.Background()); err != nil {
		return err
	}

	fmt.Println("All done.")
	return nil
}
