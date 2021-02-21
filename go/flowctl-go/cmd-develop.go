package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/testing"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdDevelop struct {
	Source    string `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Directory string `long:"directory" default:"." description:"Build directory"`
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
	}
	config.ZoneConfig.Zone = "local"
	fragment.FileSystemStoreRoot = tempdir
	pb.RegisterGRPCDispatcher(Config.Zone)

	cluster, err := testing.NewCluster(config)
	if err != nil {
		return fmt.Errorf("NewCluster: %w", err)
	}

	// Apply derivation shard specs.
	if err = todoHackedShardApply(catalog, cluster.Shards); err != nil {
		return fmt.Errorf("applying shards: %w", err)
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
