package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

// This command will be under the shards command which leverages the gazctlcmd.ShardsCfg config.
type cmdSplit struct {
	Broker        mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildsRoot    string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	Consumer      mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	DryRun        bool                  `long:"dry-run" description:"Print actions that would be taken, but don't actually take them"`
	Shard         string                `long:"shard" required:"true" description:"Shard to split"`
	SplitOnRClock bool                  `long:"split-rclock" description:"Split on rotated clock (instead of on key)"`
	Diagnostics   mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func init() {
	// Automatically register this command under the shards command
	gazctlcmd.CommandRegistry.AddCommand("shards", "split", "Split a Flow processing shard", `
Split a Flow processing shard into two, either on shuffled key or rotated clock.
`, &cmdSplit{})
}

func (cmd cmdSplit) execute(ctx context.Context) error {
	var builds, err = flow.NewBuildService(cmd.BuildsRoot)
	if err != nil {
		return err
	}

	ctx = pb.WithDispatchDefault(ctx)
	var sc = cmd.Consumer.MustShardClient(ctx)
	var jc = cmd.Broker.MustJournalClient(ctx)

	if err = pingClients(ctx, sc, jc); err != nil {
		return err
	}

	// List the identified shard.
	shardsList, err := consumer.ListShards(ctx, sc, &pc.ListRequest{
		Selector: pf.LabelSelector{Include: pb.MustLabelSet("id", cmd.Shard)},
	})
	if err != nil {
		return err
	} else if len(shardsList.Shards) != 1 {
		return fmt.Errorf("shard %s not found", cmd.Shard)
	}
	var shardSpec = shardsList.Shards[0].Spec

	// List the shard's recovery log.
	logsList, err := client.ListAllJournals(ctx, jc, pb.ListRequest{
		Selector: pf.LabelSelector{Include: pb.MustLabelSet("name", shardSpec.RecoveryLog().String())},
	})
	if err != nil {
		return err
	}

	// Load the task definition from the shard build label.
	labeling, err := labels.ParseShardLabels(shardSpec.LabelSet)
	if err != nil {
		return err
	}
	var build = builds.Open(labeling.Build)
	defer build.Close()

	var task pf.Task
	if err := build.Extract(func(db *sql.DB) error {
		switch labeling.TaskType {
		case labels.TaskTypeCapture:
			capture, err := catalog.LoadCapture(db, labeling.TaskName)
			task = capture
			return err
		case labels.TaskTypeDerivation:
			derivation, err := catalog.LoadDerivation(db, labeling.TaskName)
			task = derivation
			return err
		case labels.TaskTypeMaterialization:
			materialization, err := catalog.LoadMaterialization(db, labeling.TaskName)
			task = materialization
			return err
		default:
			panic("not reached")
		}
	}); err != nil {
		return err
	}

	desired, err := flow.MapShardToSplit(task, shardsList.Shards, !cmd.SplitOnRClock)
	if err != nil {
		return err
	}

	shards, journals, err := flow.TaskChanges(task, shardsList.Shards, logsList.Journals, desired, nil, nil)
	if err != nil {
		return err
	}
	if err = applyAllChanges(ctx, sc, jc, shards, journals, cmd.DryRun); err != nil {
		return err
	}

	return nil
}

func (cmd cmdSplit) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(gazctlcmd.ShardsCfg.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher(gazctlcmd.ShardsCfg.Zone)

	return cmd.execute(context.Background())
}
