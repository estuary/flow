package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	pc "github.com/estuary/protocols/capture"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiDelete struct {
	All            bool                  `long:"all" description:"Delete all tasks and collections"`
	AllDerivations bool                  `long:"all-derivations" description:"Activate all derivations"`
	Broker         mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID        string                `long:"build-id" required:"true" description:"ID of this build"`
	BuildsRoot     string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	Consumer       mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	DryRun         bool                  `long:"dry-run" description:"Print actions that would be taken, but don't actually take them"`
	Names          []string              `long:"name" description:"Name of task or collection to activate. May be repeated many times"`
	Network        string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Log            mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics    mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiDelete) execute(ctx context.Context) error {
	var builds, err = flow.NewBuildService(cmd.BuildsRoot)
	if err != nil {
		return err
	}

	ctx = pb.WithDispatchDefault(ctx)
	var sc = cmd.Consumer.MustShardClient(ctx)
	var jc = cmd.Broker.MustJournalClient(ctx)

	// Fetch configuration from the data plane.
	_, err = pingAndFetchConfig(ctx, sc, jc)
	if err != nil {
		return err
	}

	var build = builds.Open(cmd.BuildID)
	defer build.Close()

	// Identify collections and tasks of the build to delete.
	var collections []*pf.CollectionSpec
	var tasks []pf.Task

	if err := build.Extract(func(db *sql.DB) error {
		collections, tasks, err = loadFromCatalog(db, cmd.Names, cmd.All, cmd.AllDerivations)
		return err
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	shards, journals, err := flow.DeletionChanges(ctx, jc, sc, collections, tasks, cmd.BuildID)
	if err != nil {
		return err
	}
	if err = applyAllChanges(ctx, sc, jc, shards, journals, cmd.DryRun); err != nil {
		return err
	}

	// Remove captures from endpoints, now that we've deleted the
	// task shards that reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.CaptureSpec)
		if !ok {
			continue
		}

		driver, err := capture.NewDriver(ctx,
			spec.EndpointType, json.RawMessage(spec.EndpointSpecJson), cmd.Network, ops.StdLogger())
		if err != nil {
			return fmt.Errorf("building driver for capture %q: %w", spec.Capture, err)
		}

		response, err := driver.ApplyDelete(ctx, &pc.ApplyRequest{
			Capture: spec,
			Version: spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			DryRun:  cmd.DryRun,
		})
		if err != nil {
			return fmt.Errorf("deleting capture %q: %w", spec.Capture, err)
		}

		if response.ActionDescription != "" {
			fmt.Println("Deleting capture ", spec.Capture, ":")
			fmt.Println(response.ActionDescription)
		}

		log.WithFields(log.Fields{"name": spec.Capture}).
			Info("deleted capture from endpoint")
	}

	// Remove materializations from endpoints, now that we've deleted the
	// task shards that reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.MaterializationSpec)
		if !ok {
			continue
		}

		driver, err := materialize.NewDriver(ctx,
			spec.EndpointType, json.RawMessage(spec.EndpointSpecJson), cmd.Network, ops.StdLogger())
		if err != nil {
			return fmt.Errorf("building driver for materialization %q: %w", spec.Materialization, err)
		}

		response, err := driver.ApplyDelete(ctx, &pm.ApplyRequest{
			Materialization: spec,
			Version:         spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			DryRun:          cmd.DryRun,
		})
		if err != nil {
			return fmt.Errorf("deleting materialization %q: %w", spec.Materialization, err)
		}

		if response.ActionDescription != "" {
			fmt.Println("Deleting materialization ", spec.Materialization, ":")
			fmt.Println(response.ActionDescription)
		}

		log.WithFields(log.Fields{"name": spec.Materialization}).
			Info("deleted materialization from endpoint")
	}

	if err := build.Close(); err != nil {
		return fmt.Errorf("closing build: %w", err)
	}
	return nil
}

func (cmd apiDelete) Execute(_ []string) error {
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
