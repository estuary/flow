package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiActivate struct {
	All            bool                  `long:"all" description:"Activate all tasks and collections"`
	AllDerivations bool                  `long:"all-derivations" description:"Activate all derivations"`
	Broker         mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID        string                `long:"build-id" required:"true" description:"ID of this build"`
	BuildsRoot     string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	Consumer       mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	DryRun         bool                  `long:"dry-run" description:"Print actions that would be taken, but don't actually take them"`
	InitialSplits  int                   `long:"initial-splits" default:"1" description:"When creating new tasks, the number of initial key splits to use"`
	Names          []string              `long:"name" description:"Name of task or collection to activate. May be repeated many times"`
	Network        string                `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Log            mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics    mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiActivate) execute(ctx context.Context) error {
	var builds, err = flow.NewBuildService(cmd.BuildsRoot)
	if err != nil {
		return err
	}

	ctx = pb.WithDispatchDefault(ctx)
	var sc = cmd.Consumer.MustShardClient(ctx)
	var jc = cmd.Broker.MustJournalClient(ctx)

	// Ping to ensure connectivity.
	if err = pingClients(ctx, sc, jc); err != nil {
		return err
	}

	var build = builds.Open(cmd.BuildID)
	defer build.Close()

	// Identify collections and tasks of the build to activate.
	var collections []*pf.CollectionSpec
	var tasks []pf.Task

	if err := build.Extract(func(db *sql.DB) error {

		// Explicit names to pluck. Or, if --all or --all-derivations,
		// those are also honored.
		var names = make(map[string]struct{})
		for _, t := range cmd.Names {
			names[t] = struct{}{}
		}

		if all, err := catalog.LoadAllCollections(db); err != nil {
			return err
		} else {
			for _, c := range all {
				var _, ok = names[c.Collection.String()]
				if ok || cmd.All {
					collections = append(collections, c)
				}
			}
		}
		if all, err := catalog.LoadAllCaptures(db); err != nil {
			return err
		} else {
			for _, t := range all {
				var _, ok = names[t.TaskName()]
				if ok || cmd.All {
					tasks = append(tasks, t)
				}
			}
		}
		if all, err := catalog.LoadAllDerivations(db); err != nil {
			return err
		} else {
			for _, t := range all {
				var _, ok = names[t.TaskName()]
				if ok || cmd.All || cmd.AllDerivations {
					tasks = append(tasks, t)
				}
			}
		}
		if all, err := catalog.LoadAllMaterializations(db); err != nil {
			return err
		} else {
			for _, t := range all {
				var _, ok = names[t.TaskName()]
				if ok || cmd.All {
					tasks = append(tasks, t)
				}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	// Apply materializations to endpoints first, before we create or update the
	// task shards that will reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.MaterializationSpec)
		if !ok {
			continue
		}

		driver, err := materialize.NewDriver(ctx,
			spec.EndpointType, json.RawMessage(spec.EndpointSpecJson), cmd.Network, ops.StdLogPublisher())
		if err != nil {
			return fmt.Errorf("building driver for materialization %q: %w", spec.Materialization, err)
		}

		response, err := driver.Apply(ctx, &pm.ApplyRequest{
			Materialization: spec,
			Version:         spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			DryRun:          cmd.DryRun,
		})
		if err != nil {
			return fmt.Errorf("applying materialization %q: %w", spec.Materialization, err)
		}

		if response.ActionDescription != "" {
			fmt.Println("Applying materialization ", spec.Materialization, ":")
			fmt.Println(response.ActionDescription)
		}
		log.WithFields(log.Fields{"name": spec.Materialization}).Info("applied to endpoint")
	}

	shards, journals, err := flow.ActivationChanges(ctx, jc, sc, collections, tasks, cmd.InitialSplits)
	if err != nil {
		return err
	}
	if err = applyAllChanges(ctx, sc, jc, shards, journals, cmd.DryRun); err != nil {
		return err
	}

	// Poll task shards, waiting for them to become ready.
	for _, task := range tasks {
		var ready bool

		for attempt := 0; !ready; attempt++ {
			// Poll task shards with a back-off.
			switch attempt {
			case 0: // No-op.
			case 1, 2:
				time.Sleep(time.Millisecond * 50)
			case 3, 4:
				time.Sleep(time.Second)
			default:
				time.Sleep(time.Second * 5)
			}

			var req = flow.ListShardsRequest(task)
			var resp, err = consumer.ListShards(ctx, sc, &req)
			if err != nil {
				return fmt.Errorf("listing shards of %s: %w", task.TaskName(), err)
			}

			ready = true
			for _, shard := range resp.Shards {
				if shard.Route.Primary == -1 {
					log.WithFields(log.Fields{
						"shard": shard.Spec.Id,
					}).Info("waiting for shard to be assigned")

					ready = false
					break
				} else if code := shard.Status[shard.Route.Primary].Code; code < pc.ReplicaStatus_PRIMARY {
					log.WithFields(log.Fields{
						"shard":  shard.Spec.Id,
						"status": code,
					}).Info("waiting for shard to become ready")

					ready = false
					break
				}
			}
		}
	}
	log.Info("all shards ready")

	if err := build.Close(); err != nil {
		return fmt.Errorf("closing build: %w", err)
	}
	return nil
}

func (cmd apiActivate) Execute(_ []string) error {
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

func pingClients(ctx context.Context, sc pc.ShardClient, jc pb.JournalClient) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	if _, err := sc.List(ctx, &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", "this/shard/does/not/exist")},
	}); err != nil {
		return fmt.Errorf("pinging shard client: %w", err)
	}
	if _, err := jc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", "this/collection/does/not/exist")},
	}); err != nil {
		return fmt.Errorf("pinging journal client: %w", err)
	}

	return nil
}

func applyAllChanges(
	ctx context.Context,
	sc pc.ShardClient,
	jc pb.JournalClient,
	shards []pc.ApplyRequest_Change,
	journals []pb.ApplyRequest_Change,
	dryRun bool,
) error {

	// Stably sort journal changes so that deletions order last.
	sort.SliceStable(journals, func(i int, j int) bool {
		return journals[i].Delete == "" && journals[j].Delete != ""
	})
	// Find the first index of a journal deletion change.
	var journalPivot = sort.Search(len(journals), func(i int) bool {
		return journals[i].Delete != ""
	})

	var logJournalChange = func(c pb.ApplyRequest_Change) {
		if c.Delete != "" {
			log.WithFields(log.Fields{"name": c.Delete, "rev": c.ExpectModRevision}).Info("delete journal")
		} else if c.ExpectModRevision != 0 {
			log.WithFields(log.Fields{"name": c.Upsert.Name, "rev": c.ExpectModRevision}).Info("update journal")
		} else {
			log.WithFields(log.Fields{"name": c.Upsert.Name}).Info("insert journal")
		}
	}

	// In the first phase, apply journals which are inserted or updated.
	var phase1 = pb.ApplyRequest{Changes: journals[:journalPivot]}
	for _, j := range phase1.Changes {
		logJournalChange(j)
	}
	if !dryRun {
		if _, err := client.ApplyJournalsInBatches(ctx, jc, &phase1, maxEtcdTxnSize); err != nil {
			return fmt.Errorf("applying journals: %w", err)
		}
	}

	// In the second phase, apply all shard changes. A new shard's recovery log has already been created.
	var phase2 = pc.ApplyRequest{Changes: shards}
	for _, c := range phase2.Changes {
		if c.Delete != "" {
			log.WithFields(log.Fields{"id": c.Delete, "rev": c.ExpectModRevision}).Info("delete shard")
		} else if c.ExpectModRevision != 0 {
			log.WithFields(log.Fields{"id": c.Upsert.Id, "rev": c.ExpectModRevision}).Info("update shard")
		} else {
			log.WithFields(log.Fields{"id": c.Upsert.Id}).Info("insert shard")
		}
	}
	if !dryRun {
		if _, err := consumer.ApplyShardsInBatches(ctx, sc, &phase2, maxEtcdTxnSize); err != nil {
			return fmt.Errorf("applying shards: %w", err)
		}
	}

	// The third phase are journal deletions.
	// Recovery logs to be deleted has already had their shards removed.
	var phase3 = pb.ApplyRequest{Changes: journals[journalPivot:]}
	for _, j := range phase3.Changes {
		logJournalChange(j)
	}
	if !dryRun {
		if _, err := client.ApplyJournalsInBatches(ctx, jc, &phase3, maxEtcdTxnSize); err != nil {
			return fmt.Errorf("applying journals: %w", err)
		}
	}

	return nil
}

const maxEtcdTxnSize = 127
