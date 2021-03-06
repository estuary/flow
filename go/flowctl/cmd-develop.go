package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/testing"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdDevelop struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Poll        bool                  `long:"poll" description:"Process new, ready input from captures, and then exit"`
	Port        uint16                `long:"port" env:"PORT" default:"8080" description:"Service port for HTTP and gRPC requests"`
	Shards      int                   `long:"shards" default:"1" description:"Number of shards to create for each catalog task"`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdDevelop) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	var err error
	if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("filepath.Abs: %w", err)
	}

	var runDir = filepath.Join(cmd.Directory, "flowctl_develop")
	if err := os.MkdirAll(runDir, 0700); err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}

	// Running directory is used for:
	// * Storing our built catalog database.
	// * Etcd storage and UDS sockets.
	// * NPM worker UDS socket.
	// * Backing persisted file:/// fragments.

	built, err := buildCatalog(pf.BuildAPI_Config{
		CatalogPath:       filepath.Join(runDir, "catalog.db"),
		Directory:         cmd.Directory,
		Source:            cmd.Source,
		SourceType:        pf.ContentType_CATALOG_SPEC,
		TypescriptCompile: true,
		TypescriptPackage: false,

		// Install a testing override rule that applies after other rules,
		// disables multi-broker replication, and uses a file:// fragment store.
		ExtraJournalRules: &pf.JournalRules{
			Rules: []pf.JournalRules_Rule{
				{
					// Order after other rules.
					Rule: "\uFFFF\uFFFF-develop-overrides",
					Template: pb.JournalSpec{
						// We're running in single-process development mode.
						// Override replication (which defaults to 3).
						Replication: 1,
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	// Spawn Etcd and NPM worker processes for cluster use.
	etcd, etcdClient, err := startEtcd(runDir)
	if err != nil {
		return err
	}
	defer stopWorker(etcd)

	var lambdaJSUDS = filepath.Join(runDir, "lambda-js")
	jsWorker, err := startJSWorker(cmd.Directory, lambdaJSUDS)
	if err != nil {
		return err
	}
	defer stopWorker(jsWorker)

	// Configure and start the cluster.
	var cfg = testing.ClusterConfig{
		Context:            context.Background(),
		DisableClockTicks:  false,
		Etcd:               etcdClient,
		EtcdCatalogPrefix:  "/flowctl/develop/catalog",
		EtcdBrokerPrefix:   "/flowctl/develop/broker",
		EtcdConsumerPrefix: "/flowctl/develop/runtime",
		ServiceConfig: mbp.ServiceConfig{
			ZoneConfig: mbp.ZoneConfig{Zone: "local"},
			Host:       "localhost",
			Port:       cmd.Port,
		},
		Poll: cmd.Poll,
	}
	pb.RegisterGRPCDispatcher(cfg.ZoneConfig.Zone)

	// Apply all database materializations first, before we create or update
	// catalog entities that reference the applied tables / topics / targets.
	if err := applyMaterializations(built, false); err != nil {
		return fmt.Errorf("applying materializations: %w", err)
	}

	// Apply catalog task specifications to the cluster.
	_, catalogRevision, err := flow.ApplyCatalogToEtcd(flow.ApplyArgs{
		Ctx:                  cfg.Context,
		Etcd:                 cfg.Etcd,
		Root:                 cfg.EtcdCatalogPrefix,
		Build:                built,
		TypeScriptUDS:        lambdaJSUDS,
		TypeScriptPackageURL: "",
		DryRun:               false,
	})
	if err != nil {
		return fmt.Errorf("applying catalog to Etcd: %w", err)
	}

	fragment.FileSystemStoreRoot = filepath.Join(runDir, "fragments")
	defer client.InstallFileTransport(fragment.FileSystemStoreRoot)()

	cluster, err := testing.NewCluster(cfg)
	if err != nil {
		return fmt.Errorf("NewCluster: %w", err)
	}

	// Apply capture shard specs.
	if err = applyCaptureShards(built, cluster.Shards, cmd.Shards, catalogRevision); err != nil {
		return fmt.Errorf("applying capture shards: %w", err)
	}
	// Apply derivation shard specs.
	if err = applyDerivationShards(built, cluster.Shards, cmd.Shards, catalogRevision); err != nil {
		return fmt.Errorf("applying derivation shards: %w", err)
	}
	// Apply materialization shards.
	if err = applyMaterializationShards(built, cluster.Shards, cmd.Shards, catalogRevision); err != nil {
		return fmt.Errorf("applying materialization shards: %w", err)
	}
	cluster.WaitForShardsToAssign()

	if cmd.Poll {
		if len(built.Captures) == 0 {
			return fmt.Errorf("--poll is used, but catalog doesn't include any captures")
		}

		cluster.Consumer.Journals.KeySpace.Mu.RLock()
		var etcd = pbx.FromEtcdResponseHeader(cluster.Consumer.Journals.Header)
		cluster.Consumer.Journals.KeySpace.Mu.RUnlock()

		// Leverage a testing.Graph to track data flows through catalog tasks,
		// starting from captures. "Ingest" the capture EOF pseudo-journal to mark
		// capture tasks as having a pending stat, which is recursively tracked
		// through derivations and materializations of the catalog.
		var graph = testing.NewGraph(cluster.Consumer.Catalog.AllTasks())
		for _, capture := range built.Captures {
			graph.CompletedIngest(
				pf.Collection(capture.Capture),
				&testing.Clock{
					Etcd: etcd,
					Offsets: pb.Offsets{
						pb.Journal(fmt.Sprintf("%s/eof", capture.Capture)): 1,
					},
				},
			)
		}

		var _, err = testing.RunTestCase(graph, cluster, &pf.TestSpec{})
		if err != nil {
			log.WithField("err", err).Error("error polling the catalog")
		}
	} else {
		// We may have bound a random port. Let the user know which one.
		fmt.Println("Listening at: ", cluster.Server.Endpoint().URL())

		// Install and await signal handler.
		var signalCh = make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)
		<-signalCh
	}

	if err := cluster.Stop(); err != nil {
		return fmt.Errorf("stopping cluster: %w", err)
	}
	log.Info("goodbye")
	return nil
}
