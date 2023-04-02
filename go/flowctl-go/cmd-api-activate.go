package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pfc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
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
	Consumer       mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	DryRun         bool                  `long:"dry-run" description:"Print actions that would be taken, but don't actually take them"`
	InitialSplits  int                   `long:"initial-splits" default:"1" description:"When creating new tasks, the number of initial key splits to use"`
	Names          []string              `long:"name" description:"Name of task or collection to activate. May be repeated many times"`
	Network        string                `long:"network" description:"The Docker network that connector containers are given access to."`
	NoWait         bool                  `long:"no-wait" description:"Don't wait for all activated shards to become ready (PRIMARY)"`
	Log            mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics    mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiActivate) execute(ctx context.Context) error {
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

	// Identify collections and tasks of the build to activate.
	var collections []*pf.CollectionSpec
	var tasks []pf.Task

	if err := build.Extract(func(db *sql.DB) error {
		collections, tasks, err = loadFromCatalog(db, cmd.Names, cmd.All, cmd.AllDerivations)
		return err
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	// Apply captures to endpoints before we create or update the task shards
	// that will reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.CaptureSpec)
		if !ok {
			continue
		}
		var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
			Build:    spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			TaskName: spec.TaskName(),
			TaskType: ops.TaskType_capture,
		})

		if spec.ShardTemplate.Disable {
			log.WithField("capture", spec.Name).
				Info("Will skip applying capture because it's shards are disabled")
			continue
		}

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
			return fmt.Errorf("applying capture %q: %w", spec.Name, err)
		}

		if response.Applied != nil && response.Applied.ActionDescription != "" {
			fmt.Println("Applying capture ", spec.Name, ":")
			fmt.Println(response.Applied.ActionDescription)
		}
		log.WithFields(log.Fields{"name": spec.Name}).Info("applied capture to endpoint")
	}

	// As with captures, apply materializations before we create or update the
	// task shards that reference them.
	for _, t := range tasks {
		var spec, ok = t.(*pf.MaterializationSpec)
		if !ok {
			continue
		}
		var publisher = ops.NewLocalPublisher(labels.ShardLabeling{
			Build:    spec.ShardTemplate.LabelSet.ValueOf(labels.Build),
			TaskName: spec.TaskName(),
			TaskType: ops.TaskType_materialization,
		})

		if spec.ShardTemplate.Disable {
			log.WithField("materialization", spec.Name).
				Info("Will skip applying materialization because it's shards are disabled")
			continue
		}

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
			return fmt.Errorf("applying materialization %q: %w", spec.Name, err)
		}

		if response.Applied != nil && response.Applied.ActionDescription != "" {
			fmt.Println("Applying materialization ", spec.Name, ":")
			fmt.Println(response.Applied.ActionDescription)
		}
		log.WithFields(log.Fields{"name": spec.Name}).Info("applied materialization to endpoint")
	}

	shards, journals, err := flow.ActivationChanges(ctx, rjc, sc, collections, tasks, cmd.InitialSplits)
	if err != nil {
		return err
	}
	if err = applyAllChanges(ctx, sc, rjc, shards, journals, cmd.DryRun); err == errNoChangesToApply {
		log.Info("there are no changes to apply")
	} else if err != nil {
		return err
	}

	// Unassign any failed shards as a part of activating this publication.
	if !cmd.DryRun {
		ids := []pc.ShardID{}
		for _, shard := range shards {
			if shard.Upsert != nil {
				ids = append(ids, shard.Upsert.Id)
			}
		}

		if _, err := sc.Unassign(ctx, &pc.UnassignRequest{
			Shards:     ids,
			OnlyFailed: true,
		}); err != nil {
			return fmt.Errorf("unassigning failed shards: %w", err)
		}
	}

	// Poll task shards, waiting for them to become ready.
	for _, task := range tasks {

		if task.TaskShardTemplate().Disable {
			log.WithField("task", task.TaskName()).Info("task is disabled")
			continue
		}

		var ready bool
		for attempt := 0; !ready && !cmd.NoWait; attempt++ {
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
	if !cmd.NoWait {
		log.Info("all shards ready")
	}

	if err := build.Close(); err != nil {
		return fmt.Errorf("closing build: %w", err)
	}
	return nil
}

func (cmd apiActivate) Execute(_ []string) error {
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

func newJournalClient(ctx context.Context, broker mbp.ClientConfig) (pb.RoutedJournalClient, *pb.Header_Etcd, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	var jc = broker.MustRoutedJournalClient(ctx)

	resp, err := jc.List(ctx, &pb.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("name", "this/collection/does/not/exist")},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pinging journal client: %w", err)
	}
	return jc, &resp.Header.Etcd, nil
}

func newShardClient(ctx context.Context, consumer mbp.ClientConfig) (pc.ShardClient, *pb.Header_Etcd, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	var sc = consumer.MustShardClient(ctx)

	resp, err := sc.List(ctx, &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet("id", "this/shard/does/not/exist")},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pinging shard client: %w", err)
	}

	return sc, &resp.Header.Etcd, nil
}

func getBuildsRoot(ctx context.Context, consumer mbp.ClientConfig) (*url.URL, error) {
	var resource = consumer.Address.URL()
	var client = http.DefaultClient

	if resource.Scheme == "unix" {
		var socketPath = resource.Path

		client = &http.Client{
			Transport: &http.Transport{
				DialTLSContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		}
		resource.Scheme = "https"
	}

	// We can determine the configured --flow.builds-root of the data plane consumer
	// by asking a random member through the /debug/vars interface.
	// This is a little gross, but... shrug.
	var vars struct {
		Cmdline []string
	}
	resource.Path = "/debug/vars"

	resp, err := client.Get(resource.String())
	if err != nil {
		return nil, fmt.Errorf("fetching consumer vars: %w", err)
	} else if resp.StatusCode != 200 {
		return nil, fmt.Errorf("fetching consumer vars: %s", resp.Status)
	}
	defer resp.Body.Close()

	if err = json.NewDecoder(resp.Body).Decode(&vars); err != nil {
		return nil, fmt.Errorf("decoding consumer vars: %w", err)
	}

	var s string
	for i := range vars.Cmdline {
		if i > 0 && vars.Cmdline[i-1] == "--flow.builds-root" {
			s = vars.Cmdline[i]
		}
	}
	if s == "" {
		return nil, fmt.Errorf("empty builds root (consumer cmdline: %v)", vars.Cmdline)
	}

	return url.Parse(s)
}

// loadFromCatalog loads collections and tasks in |names| from the catalog.
// If |allDerivations|, then all derivations are also loaded.
// If |all|, then all entities are loaded.
func loadFromCatalog(db *sql.DB, names []string, all, allDerivations bool) ([]*pf.CollectionSpec, []pf.Task, error) {
	var idx = make(map[string]int)
	for _, t := range names {
		idx[t] = 0
	}

	var collections []*pf.CollectionSpec
	var tasks []pf.Task

	if loaded, err := catalog.LoadAllCollections(db); err != nil {
		return nil, nil, err
	} else {
		for _, c := range loaded {
			var name = c.Name.String()
			var _, ok = idx[name]
			if ok || all || allDerivations {
				collections = append(collections, c)
				idx[name] = idx[name] + 1

				if c.Derivation != nil {
					tasks = append(tasks, c)
				}
			}
		}
	}
	if loaded, err := catalog.LoadAllCaptures(db); err != nil {
		return nil, nil, err
	} else {
		for _, t := range loaded {
			var _, ok = idx[t.TaskName()]
			if ok || all {
				tasks = append(tasks, t)
				idx[t.TaskName()] = idx[t.TaskName()] + 1
			}
		}
	}
	if loaded, err := catalog.LoadAllMaterializations(db); err != nil {
		return nil, nil, err
	} else {
		for _, t := range loaded {
			var _, ok = idx[t.TaskName()]
			if ok || all {
				tasks = append(tasks, t)
				idx[t.TaskName()] = idx[t.TaskName()] + 1
			}
		}
	}

	// Require that all |names| were matched.
	for n, c := range idx {
		if c == 0 {
			return nil, nil, fmt.Errorf("could not find %q in the build database", n)
		}
	}

	return collections, tasks, nil
}

func applyAllChanges(
	ctx context.Context,
	sc pc.ShardClient,
	jc pb.JournalClient,
	shards []pc.ApplyRequest_Change,
	journals []pb.ApplyRequest_Change,
	dryRun bool,
) error {

	if len(shards) == 0 && len(journals) == 0 {
		return errNoChangesToApply
	}

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
	if !dryRun && len(phase1.Changes) != 0 {
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
	if !dryRun && len(phase2.Changes) != 0 {
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
	if !dryRun && len(phase3.Changes) != 0 {
		if _, err := client.ApplyJournalsInBatches(ctx, jc, &phase3, maxEtcdTxnSize); err != nil {
			return fmt.Errorf("applying journals: %w", err)
		}
	}

	return nil
}

const maxEtcdTxnSize = 127

var errNoChangesToApply = fmt.Errorf("there are no changes to apply")
