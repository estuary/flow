package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/testing"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiAwait struct {
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID     string                `long:"build-id" required:"true" description:"ID of this build"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiAwait) execute(ctx context.Context) error {
	ctx = pb.WithDispatchDefault(ctx)

	rjc, _, err := newJournalClient(ctx, cmd.Broker)
	if err != nil {
		return err
	}
	sc, _, err := newShardClient(ctx, cmd.Consumer)
	if err != nil {
		return err
	}
	buildsRoot, err := getBuildsRoot(ctx, cmd.Consumer)
	if err != nil {
		return err
	}
	builds, err := flow.NewBuildService(buildsRoot.String())
	if err != nil {
		return err
	}

	var build = builds.Open(cmd.BuildID)
	defer build.Close()

	// Load collections and tasks.
	var collections []*pf.CollectionSpec
	var captures []*pf.CaptureSpec
	var materializations []*pf.MaterializationSpec

	if err := build.Extract(func(db *sql.DB) error {
		if collections, err = catalog.LoadAllCollections(db); err != nil {
			return err
		}
		if captures, err = catalog.LoadAllCaptures(db); err != nil {
			return err
		}
		if materializations, err = catalog.LoadAllMaterializations(db); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	// Build a testing graph and driver to track dataflow execution.
	// It doesn't require a TestingClient given how we'll use it (no actual tests).
	driver, err := testing.NewClusterDriver(ctx, sc, rjc, nil, cmd.BuildID, collections)
	if err != nil {
		return fmt.Errorf("building test driver: %w", err)
	}
	var graph = testing.NewGraph(captures, collections, materializations)

	// "Ingest" the capture EOF pseudo-journal to mark
	// capture tasks as having a pending stat, which is recursively tracked
	// through derivations and materializations of the catalog.
	for _, capture := range captures {
		graph.CompletedIngest(
			pf.Collection(capture.Name),
			pb.Offsets{pb.Journal(fmt.Sprintf("%s/eof", capture.Name)): 1},
		)
	}
	// Initialize fetches current collection offsets, and waits for the dataflow
	// execution to fully settle (including our ingested capture EOFs).
	if err = testing.Initialize(ctx, driver, graph); err != nil {
		return fmt.Errorf("initializing dataflow tracking: %w", err)
	}

	if err := build.Close(); err != nil {
		return fmt.Errorf("closing build: %w", err)
	}
	return nil
}

func (cmd apiAwait) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)
	var ctx, cancelFn = context.WithTimeout(context.Background(), executeTimeout)
	defer cancelFn()

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Debug("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	return cmd.execute(ctx)
}
