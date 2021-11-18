package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/testing"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiAwait struct {
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID     string                `long:"build-id" required:"true" description:"ID of this build"`
	BuildsRoot  string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiAwait) execute(ctx context.Context) error {
	var builds, err = flow.NewBuildService(cmd.BuildsRoot)
	if err != nil {
		return err
	}

	ctx = pb.WithDispatchDefault(ctx)
	var sc = cmd.Consumer.MustShardClient(ctx)
	var rjc = cmd.Broker.MustRoutedJournalClient(ctx)

	// Fetch configuration from the data plane.
	config, err := pingAndFetchConfig(ctx, sc, rjc)
	if err != nil {
		return err
	}

	var build = builds.Open(cmd.BuildID)
	defer build.Close()

	// Load collections and tasks.
	var collections []*pf.CollectionSpec
	var captures []*pf.CaptureSpec
	var derivations []*pf.DerivationSpec
	var materializations []*pf.MaterializationSpec

	if err := build.Extract(func(db *sql.DB) error {
		if collections, err = catalog.LoadAllCollections(db); err != nil {
			return err
		}
		if captures, err = catalog.LoadAllCaptures(db); err != nil {
			return err
		}
		if derivations, err = catalog.LoadAllDerivations(db); err != nil {
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
	// It doesn't require a schema bundle or testing client given how we'll use it (no actual tests).
	driver, err := testing.NewClusterDriver(ctx, sc, rjc, nil, cmd.BuildID, &pf.SchemaBundle{}, collections)
	if err != nil {
		return fmt.Errorf("building test driver: %w", err)
	}
	var graph = testing.NewGraph(captures, derivations, materializations)

	// "Ingest" the capture EOF pseudo-journal to mark
	// capture tasks as having a pending stat, which is recursively tracked
	// through derivations and materializations of the catalog.
	for _, capture := range captures {
		if capture.EndpointType == pf.EndpointType_INGEST {
			continue // Skip ingestions, which never EOF.
		}

		graph.CompletedIngest(
			pf.Collection(capture.Capture),
			&testing.Clock{
				Etcd:    config.JournalsEtcd,
				Offsets: pb.Offsets{pb.Journal(fmt.Sprintf("%s/eof", capture.Capture)): 1},
			},
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

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	return cmd.execute(context.Background())
}
