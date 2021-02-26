package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/estuary/flow/go/flow"
	flowLabels "github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize/driver"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/testing"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdDevelop struct {
	Source    string `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Directory string `long:"directory" default:"." description:"Build directory"`
	mbp.ServiceConfig
}

func (cmd cmdDevelop) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	mbp.InitLog(Config.Log)

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	// Create a temp directory, used for:
	// * Storing our built catalog database.
	// * Etcd storage and UDS sockets.
	// * NPM worker UDS socket.
	// * "Persisted" fragment files.

	tempdir, err := ioutil.TempDir("", "flow-test")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(tempdir)

	var buildConfig = pf.BuildAPI_Config{
		CatalogPath:       filepath.Join(tempdir, "catalog.db"),
		Directory:         cmd.Directory,
		Source:            cmd.Source,
		TypescriptCompile: true,
		TypescriptPackage: false,

		// Install a testing override rule that applies after other rules,
		// disables multi-broker replication, and uses a file:// fragment store.
		ExtraJournalRules: &pf.JournalRules{
			Rules: []pf.JournalRules_Rule{
				{
					// Order after other rules.
					Rule: "\uFFFF\uFFFF-testing-overrides",
					Template: pb.JournalSpec{
						Replication: 1,
						Fragment: pb.JournalSpec_Fragment{
							Stores:           []pb.FragmentStore{"file:///"},
							CompressionCodec: pb.CompressionCodec_SNAPPY,
						},
					},
				},
			},
		},
	}
	catalog, err := build(buildConfig)
	if err != nil {
		return fmt.Errorf("building catalog: %w", err)
	}

	// Spawn Etcd and NPM worker processes for cluster use.
	etcd, etcdClient, err := startTempEtcd(tempdir)
	if err != nil {
		return err
	}
	defer stopWorker(etcd)

	var lambdaJSUDS = filepath.Join(tempdir, "lambda-js")
	npmWorker, err := startNpmWorker(lambdaJSUDS)
	if err != nil {
		return err
	}
	defer stopWorker(npmWorker)

	// Configure and start the cluster.
	var config = testing.ClusterConfig{
		Context:           context.Background(),
		Catalog:           catalog,
		DisableClockTicks: true,
		Etcd:              etcdClient,
		LambdaJSUDS:       lambdaJSUDS,
		ServiceConfig:     cmd.ServiceConfig,
	}
	fragment.FileSystemStoreRoot = tempdir
	pb.RegisterGRPCDispatcher(Config.Zone)

	cluster, err := testing.NewCluster(config)
	if err != nil {
		return fmt.Errorf("NewCluster: %w", err)
	}

	// Apply derivation shard specs.
	if err = todoHackedDeriveApply(catalog, cluster.Shards); err != nil {
		return fmt.Errorf("applying shards: %w", err)
	}
	// Apply materializations.
	if err = todoHackedMaterializeApply(catalog, cluster.Shards); err != nil {
		return fmt.Errorf("applying materializations: %w", err)
	}

	// Install and await signal handler.
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
	<-signalCh

	if err := cluster.Stop(); err != nil {
		return fmt.Errorf("stopping cluster: %w", err)
	}
	log.Info("goodbye")
	return nil
}

func todoHackedMaterializeApply(catalog *flow.Catalog, shards pc.ShardClient) error {
	names, err := catalog.LoadMaterializationNames()
	if err != nil {
		return fmt.Errorf("loading materialization names: %w", err)
	}

	for _, name := range names {
		var spec, err = catalog.LoadMaterialization(name)
		if err != nil {
			return fmt.Errorf("loading materialization: %w", err)
		}

		driver, err := driver.NewDriver(context.Background(),
			spec.EndpointType, json.RawMessage(spec.EndpointConfig))
		if err != nil {
			return fmt.Errorf("driver.NewDriver: %w", err)
		}

		response, err := driver.Apply(context.Background(), &pm.ApplyRequest{
			Materialization: spec,
			DryRun:          false,
		})
		if err != nil {
			return fmt.Errorf("driver.Apply: %w", err)
		}

		fmt.Println(response.ActionDescription)
	}

	log.WithField("names", names).Info("building materialization shard specs")
	var changes []pc.ApplyRequest_Change

	for _, name := range names {
		var labels = pb.MustLabelSet(
			labels.ManagedBy, flowLabels.ManagedByFlow,
			flowLabels.CatalogURL, catalog.LocalPath(),
			flowLabels.Materialization, name,
			flowLabels.KeyBegin, flowLabels.KeyBeginMin,
			flowLabels.KeyEnd, flowLabels.KeyEndMax,
			flowLabels.RClockBegin, flowLabels.RClockBeginMin,
			flowLabels.RClockEnd, flowLabels.RClockEndMax,
		)
		changes = append(changes, pc.ApplyRequest_Change{
			Upsert: &pc.ShardSpec{
				Id: pc.ShardID(fmt.Sprintf("materialize/%s/%s-%s",
					name, flowLabels.KeyBeginMin, flowLabels.RClockBeginMin)),
				Sources:           nil,
				RecoveryLogPrefix: "recovery",
				HintPrefix:        "/estuary/flow/hints",
				HintBackups:       2,
				MaxTxnDuration:    time.Second,
				MinTxnDuration:    0,
				HotStandbys:       0,
				LabelSet:          labels,
			},
			ExpectModRevision: 0,
		})
	}

	if _, err = consumer.ApplyShards(context.Background(), shards, &pc.ApplyRequest{
		Changes: changes,
	}); err != nil {
		return fmt.Errorf("applying shard specs: %w", err)
	}
	return nil
}
