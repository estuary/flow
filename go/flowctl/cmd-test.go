package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/testing"
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
	"google.golang.org/grpc"
)

type cmdTest struct {
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	Shards      int                   `long:"shards" default:"1" description:"Number of shards for each tested derivation"`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdTest) Execute(_ []string) (retErr error) {
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

	runDir, err := ioutil.TempDir("", "flow-test")
	if err != nil {
		return fmt.Errorf("creating temp directory: %w", err)
	}
	defer os.RemoveAll(runDir)

	// Temporary running directory, used for:
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
		DisableClockTicks:  true, // Test driver advances synthetic time.
		Etcd:               etcdClient,
		EtcdCatalogPrefix:  "/flowctl/test/catalog",
		EtcdBrokerPrefix:   "/flowctl/test/broker",
		EtcdConsumerPrefix: "/flowctl/test/runtime",
		ServiceConfig: mbp.ServiceConfig{
			ZoneConfig: mbp.ZoneConfig{Zone: "local"},
			Host:       "localhost",
			Port:       0, // Any available port.
		},
	}
	pb.RegisterGRPCDispatcher(cfg.ZoneConfig.Zone)

	// Apply catalog task specifications to the cluster.
	_, catalogRevision, err := flow.ApplyCatalogToEtcd(flow.ApplyArgs{
		Ctx:           cfg.Context,
		Etcd:          cfg.Etcd,
		Root:          cfg.EtcdCatalogPrefix,
		Build:         built,
		TypeScriptUDS: lambdaJSUDS,
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

	// Apply derivation shard specs.
	if err = applyDerivationShards(built, cluster.Shards, cmd.Shards, catalogRevision); err != nil {
		return fmt.Errorf("applying derivation shards: %w", err)
	}
	cluster.WaitForShardsToAssign()

	// Run all test cases ordered by their scope, which implicitly orders on resource file and test name.
	sort.Slice(built.Tests, func(i, j int) bool {
		return built.Tests[i].Steps[0].StepScope < built.Tests[j].Steps[0].StepScope
	})

	var graph = testing.NewGraph(cluster.Consumer.Catalog.AllTasks())
	var failed []string
	fmt.Println("Running ", len(built.Tests), " tests...")

	for _, testCase := range built.Tests {
		if scope, err := testing.RunTestCase(graph, cluster, &testCase); err != nil {
			var path, ptr = scopeToPathAndPtr(cmd.Directory, scope)
			fmt.Println("❌", yellow(path), "failure at step", red(ptr), ":")
			fmt.Println(err)
			failed = append(failed, testCase.Test)
		} else {
			var path, _ = scopeToPathAndPtr(cmd.Directory, testCase.Steps[0].StepScope)
			fmt.Println("✔️", path, "::", green(testCase.Test))
		}
		cluster.Consumer.ClearRegistersForTest(cfg.Context)
	}

	fmt.Printf("\nRan %d tests, %d passed, %d failed\n",
		len(built.Tests), len(built.Tests)-len(failed), len(failed))

	if err := cluster.Stop(); err != nil {
		return fmt.Errorf("stopping cluster: %w", err)
	}
	if failed != nil {
		return fmt.Errorf("failed tests: [%s]", strings.Join(failed, ", "))
	}
	return nil
}

func startEtcd(tmpdir string) (*exec.Cmd, *clientv3.Client, error) {
	var cmd = exec.Command("etcd",
		"--listen-peer-urls", "unix://peer.sock:0",
		"--listen-client-urls", "unix://client.sock:0",
		"--advertise-client-urls", "unix://client.sock:0",
	)
	// The Etcd --log-level flag was added in v3.4. Use it's environment variable
	// version to remain compatible with older `etcd` binaries.
	cmd.Env = append(cmd.Env, "ETCD_LOG_LEVEL=error", "ETCD_LOGGER=zap")
	cmd.Env = append(cmd.Env, os.Environ()...)

	cmd.Dir = tmpdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Deliver a SIGTERM to the process if this thread should die uncleanly.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	// Place child its own process group, so that terminal SIGINT isn't delivered
	// from the terminal and so that we may close leases properly.
	cmd.SysProcAttr.Setpgid = true

	log.WithFields(log.Fields{"args": cmd.Args, "dir": cmd.Dir}).Info("starting etcd")
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting etcd: %w", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"unix://" + cmd.Dir + "/client.sock:0"},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		// Require a reasonably recent server cluster.
		RejectOldCluster: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("building etcd client: %w", err)
	}

	var ctx = context.Background()

	// Look for any left-over leases of a prior invocation, and remove them.
	leases, err := etcdClient.Leases(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching existing leases: %w", err)
	}
	for _, lease := range leases.Leases {
		if _, err := etcdClient.Revoke(ctx, lease.ID); err != nil {
			return nil, nil, fmt.Errorf("revoking existing lease: %w", err)
		}
		log.WithField("lease", lease.ID).
			Warn("removed an existing Etcd lease (unclean shutdown?)")
	}

	// Arrange to close the |etcdClient| as soon as the process completes.
	// We do this because ctrl-C sent to `flowctl develop` will also immediately
	// propagate to the `etcd` binary; as part of normal shutdown we'll try to
	// release associated Etcd leases, and will wedge for ~10 seconds trying to
	// do so before timing out and bailing out.
	go func() {
		_, _ = cmd.Process.Wait()
		etcdClient.Close()
	}()

	return cmd, etcdClient, nil
}

func startJSWorker(dir, socketPath string) (*exec.Cmd, error) {
	return flow.StartCmdAndReadReady(dir, socketPath,
		false, // Use process group of parent. Terminal signals pass through.
		"node", "dist/flow_generated/flow/main.js")
}

func stopWorker(cmd *exec.Cmd) {
	_ = cmd.Process.Signal(syscall.SIGTERM)
	_ = cmd.Wait()
}

var green = color.New(color.FgGreen).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()
