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
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiDelete struct {
	All            bool                  `long:"all" description:"Delete all tasks and collections"`
	AllDerivations bool                  `long:"all-derivations" description:"Delete all derivations"`
	Broker         mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID        string                `long:"build-id" required:"true" description:"ID of this build"`
	Consumer       mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	DryRun         bool                  `long:"dry-run" description:"Print actions that would be taken, but don't actually take them"`
	Names          []string              `long:"name" description:"Name of task or collection to activate. May be repeated many times"`
	Network        string                `long:"network" description:"The Docker network that connector containers are given access to."`
	Log            mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics    mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiDelete) execute(ctx context.Context) error {
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

	// Identify collections and tasks of the build to delete.
	var collections []*pf.CollectionSpec
	var tasks []pf.Task

	if err := build.Extract(func(db *sql.DB) error {
		collections, tasks, err = loadFromCatalog(db, cmd.Names, cmd.All, cmd.AllDerivations)
		return err
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	shards, journals, err := flow.DeletionChanges(ctx, rjc, sc, collections, tasks)
	if err != nil {
		return err
	}
	if err = applyAllChanges(ctx, sc, rjc, shards, journals, cmd.DryRun); err == errNoChangesToApply {
		log.Warn("there are no changes to apply")
	} else if err != nil {
		return err
	}

	// Remove captures from endpoints, now that we've deleted the
	// task shards that reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.CaptureSpec)
		if !ok {
			continue
		}
		if spec.ShardTemplate.Disable {
			log.WithField("capture", spec.Capture.String()).
				Info("Will skip un-applying capture because it's disabled")
			continue
		}

		driver, err := capture.NewDriver(ctx,
			spec.EndpointType, json.RawMessage(spec.EndpointSpecJson), cmd.Network, spec.ShardTemplate.Id.String(), ops.StdLogger())
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
		if spec.ShardTemplate.Disable {
			log.WithField("materialization", spec.Materialization.String()).
				Info("Will skip un-applying materialization because it's disabled")
			continue
		}

		driver, err := materialize.NewDriver(ctx,
			spec.EndpointType, json.RawMessage(spec.EndpointSpecJson), cmd.Network, spec.ShardTemplate.Id.String(), ops.StdLogger())
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
	var ctx, cancelFn = context.WithTimeout(context.Background(), executeTimeout)
	defer cancelFn()

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	return cmd.execute(ctx)
}
