package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/cmd/gazctl/gazctlcmd"
	mbp "go.gazette.dev/core/mainboilerplate"
)

// This command will be under the journals command which leverages the gazctlcmd.JournalsCfg config.
type cmdJournalsSplit struct {
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	DryRun      bool                  `long:"dry-run" description:"Print actions that would be taken, but don't actually take them"`
	Journal     string                `long:"journal" required:"true" description:"Journal to split"`
	Splits      uint                  `long:"splits" default:"2" description:"Number of splits, as a power-of-two greater than or equal to two"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func init() {
	// Automatically register this command under the journals command
	gazctlcmd.CommandRegistry.AddCommand("journals", "split", "Split a Flow collection journal", `
Split a Flow collection journal into two.
`, &cmdJournalsSplit{})
}

func (cmd cmdJournalsSplit) execute(ctx context.Context) error {
	ctx = pb.WithDispatchDefault(ctx)

	rjc, _, err := newJournalClient(ctx, gazctlcmd.JournalsCfg.Broker)
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

	// List the identified journal.
	journalsList, err := client.ListAllJournals(ctx, rjc, pb.ListRequest{
		Selector: pf.LabelSelector{Include: pb.MustLabelSet("name", cmd.Journal)},
	})
	if err != nil {
		return err
	} else if len(journalsList.Journals) != 1 {
		return fmt.Errorf("journal %s not found", cmd.Journal)
	}
	var journalSpec = journalsList.Journals[0].Spec

	// Load the collection definition from the build and collection labels.
	buildID, err := labels.ExpectOne(journalSpec.LabelSet, labels.Build)
	if err != nil {
		return err
	}
	collection, err := labels.ExpectOne(journalSpec.LabelSet, labels.Collection)
	if err != nil {
		return err
	}

	var build = builds.Open(buildID)
	defer build.Close()

	var collectionSpec *pf.CollectionSpec
	if err := build.Extract(func(db *sql.DB) (err error) {
		collectionSpec, err = catalog.LoadCollection(db, collection)
		return
	}); err != nil {
		return err
	}

	desired, err := flow.MapPartitionToSplit(collectionSpec, journalsList.Journals, cmd.Splits)
	if err != nil {
		return err
	}

	changes, err := flow.CollectionChanges(collectionSpec, journalsList.Journals, desired, nil)
	if err != nil {
		return err
	}
	if err = applyAllChanges(ctx, nil, rjc, nil, changes, cmd.DryRun); err != nil {
		return err
	}

	return nil
}

func (cmd cmdJournalsSplit) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(gazctlcmd.JournalsCfg.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Debug("flowctl configuration")
	pb.RegisterGRPCDispatcher(gazctlcmd.ShardsCfg.Zone)

	return cmd.execute(context.Background())
}
