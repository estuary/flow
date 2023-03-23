package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	pfc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	po "github.com/estuary/flow/go/protocols/ops"
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
		log.Info("there are no changes to apply")
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
		var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
			Build:    spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			TaskName: spec.TaskName(),
			TaskType: po.Shard_capture,
		})

		if spec.ShardTemplate.Disable {
			log.WithField("capture", spec.Name.String()).
				Info("Will skip deleting capture because it's disabled")
			continue
		}

		// Communicate a deletion to the connector as a semantic apply of this capture with no bindings.
		spec.Bindings = nil

		var request = &pfc.Request{
			Apply: &pfc.Request_Apply{
				Capture: spec,
				Version: publisher.Labels().Build,
				DryRun:  cmd.DryRun,
			},
		}
		var response, err = connector.Invoke[pfc.Response](
			ctx,
			request,
			cmd.Network,
			publisher,
			func(driver *connector.Driver) (pfc.Connector_CaptureClient, error) {
				return driver.CaptureClient().Capture(ctx)
			},
		)
		if err != nil {
			return fmt.Errorf("deleting capture %q: %w", spec.Name, err)
		}

		if response.Applied != nil && response.Applied.ActionDescription != "" {
			fmt.Println("Deleting capture ", spec.Name, ":")
			fmt.Println(response.Applied.ActionDescription)
		}

		log.WithFields(log.Fields{"name": spec.Name}).Info("deleted capture from endpoint")
	}

	// Remove materializations from endpoints, now that we've deleted the
	// task shards that reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.MaterializationSpec)
		if !ok {
			continue
		}
		var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
			Build:    spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			TaskName: spec.TaskName(),
			TaskType: po.Shard_materialization,
		})

		if spec.ShardTemplate.Disable {
			log.WithField("materialization", spec.Name.String()).
				Info("Will skip deleting materialization because it's disabled")
			continue
		}

		// Communicate a deletion to the connector as a semantic apply of this materialization with no bindings.
		spec.Bindings = nil

		var request = &pm.Request{
			Apply: &pm.Request_Apply{
				Materialization: spec,
				Version:         publisher.Labels().Build,
				DryRun:          cmd.DryRun,
			},
		}
		var response, err = connector.Invoke[pm.Response](
			ctx,
			request,
			cmd.Network,
			publisher,
			func(driver *connector.Driver) (pm.Connector_MaterializeClient, error) {
				return driver.MaterializeClient().Materialize(ctx)
			},
		)
		if err != nil {
			return fmt.Errorf("deleting materialization %q: %w", spec.Name, err)
		}

		if response.Applied != nil && response.Applied.ActionDescription != "" {
			fmt.Println("Deleting materialization ", spec.Name, ":")
			fmt.Println(response.Applied.ActionDescription)
		}

		log.WithFields(log.Fields{"name": spec.Name}).
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
