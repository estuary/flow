package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/flow"
	flowLabels "github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/runtime"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
	"google.golang.org/grpc"
)

type cmdApply struct {
	Additive    bool                  `long:"additive" description:"Add and update catalog tasks, but don't remove existing tasks not in this applied catalog"`
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	DryRun      bool                  `long:"dry-run" description:"Dry run, don't actually apply"`
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Flow        runtime.FlowConfig    `group:"Flow" namespace:"flow" env-namespace:"FLOW"`
	Etcd        mbp.EtcdConfig        `group:"Etcd" namespace:"etcd" env-namespace:"ETCD"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd cmdApply) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	var err error
	if cmd.Directory, err = filepath.Abs(cmd.Directory); err != nil {
		return fmt.Errorf("filepath.Abs: %w", err)
	}
	var ctx = context.Background()

	built, err := buildCatalog(ctx, pf.BuildAPI_Config{
		CatalogPath:       filepath.Join(cmd.Directory, "catalog.db"),
		Directory:         cmd.Directory,
		Source:            cmd.Source,
		SourceType:        pf.ContentType_CATALOG_SPEC,
		TypescriptPackage: true,
	})
	if err != nil {
		return err
	} else if len(built.NPMPackage) == 0 {
		panic("built with TypescriptPackage: true but NPM package is empty")
	}

	var shards = cmd.Consumer.MustRoutedShardClient(ctx)

	// We don't use Etcd.MustDial because that syncs the Etcd cluster,
	// and we may be running behind a port-forward which doesn't have
	// direct access to advertised Etcd member addresses.
	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{string(cmd.Etcd.Address)},
		DialTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	})
	if err != nil {
		return fmt.Errorf("dialing Etcd: %w", err)
	}

	// Apply all database materializations first, before we create or update
	// catalog entities that reference the applied tables / topics / targets.
	if err := applyMaterializations(ctx, built, cmd.DryRun); err != nil {
		return fmt.Errorf("applying materializations: %w", err)
	}

	// Install NPM package as an etcd:// key that we'll reference.
	var packageSum = sha1.Sum(built.NPMPackage)
	var packageKey = fmt.Sprintf("/flow/npm-package/%s-%x",
		time.Now().Format(time.RFC3339), hex.EncodeToString(packageSum[:8]))

	if !cmd.DryRun {
		if _, err := etcd.Put(ctx, packageKey, string(built.NPMPackage)); err != nil {
			return fmt.Errorf("storing NPM package to etcd: %w", err)
		}
	}

	// Apply catalog task specifications to the cluster.
	commons, revision, err := flow.ApplyCatalogToEtcd(flow.ApplyArgs{
		Ctx:                  ctx,
		Etcd:                 etcd,
		Root:                 cmd.Flow.CatalogRoot,
		Build:                built,
		TypeScriptUDS:        "",
		TypeScriptPackageURL: "etcd://" + packageKey,
		DryRun:               cmd.DryRun,
		Prune:                !cmd.Additive,
	})
	if err != nil {
		return fmt.Errorf("applying catalog to Etcd: %w", err)
	}
	log.WithFields(log.Fields{
		"commons":  commons,
		"revision": revision,
	}).Info("applied catalog to Etcd")

	if !cmd.DryRun {
		// Apply capture shard specs.
		if err = applyCaptureShards(ctx, built, shards, 1, revision); err != nil {
			return fmt.Errorf("applying capture shards: %w", err)
		}
		// Apply derivation shard specs.
		if err = applyDerivationShards(ctx, built, shards, 1, revision); err != nil {
			return fmt.Errorf("applying derivation shards: %w", err)
		}
		// Apply materialization shards.
		if err = applyMaterializationShards(ctx, built, shards, 1, revision); err != nil {
			return fmt.Errorf("applying materialization shards: %w", err)
		}
		fmt.Println("Applied.")
	} else {
		fmt.Println("Not applied (dry run).")
	}

	return err
}

func buildCatalog(ctx context.Context, config pf.BuildAPI_Config) (*bindings.BuiltCatalog, error) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		Context:             ctx,
		BuildAPI_Config:     config,
		FileRoot:            "/",
		CaptureDriverFn:     capture.NewDriver,
		MaterializeDriverFn: materialize.NewDriver,
	})
	if err != nil {
		return nil, fmt.Errorf("building catalog: %w", err)
	}

	for _, be := range built.Errors {
		var path, ptr = scopeToPathAndPtr(config.Directory, be.Scope)
		fmt.Println(yellow(path), "error at", red(ptr), ":")
		fmt.Println(be.Error)
	}

	if len(built.Errors) != 0 {
		return nil, fmt.Errorf("%d build errors", len(built.Errors))
	}
	return built, nil
}

func scopeToPathAndPtr(dir, scope string) (path, ptr string) {
	u, err := url.Parse(scope)
	if err != nil {
		panic(err)
	}

	ptr, u.Fragment = u.Fragment, ""
	path = u.String()

	if u.Scheme == "file" && strings.HasPrefix(u.Path, dir) {
		path = path[len(dir)+len("file://")+1:]
	}
	if ptr == "" {
		ptr = "<root>"
	}
	return path, ptr
}

func applyCaptureShards(ctx context.Context, built *bindings.BuiltCatalog, client pc.ShardClient, shards int, commonsRevision int64) error {
	for _, spec := range built.Captures {
		if err := createTaskShards(ctx,
			client,
			flowLabels.TaskTypeCapture,
			spec.Capture.String(),
			shards,
			commonsRevision,
		); err != nil {
			return err
		}
	}
	return nil
}

func applyDerivationShards(ctx context.Context, built *bindings.BuiltCatalog,
	client pc.ShardClient, shards int, commonsRevision int64) error {
	for _, spec := range built.Derivations {
		if err := createTaskShards(ctx, client,
			flowLabels.TaskTypeDerivation,
			spec.Collection.Collection.String(),
			shards,
			commonsRevision,
		); err != nil {
			return err
		}
	}
	return nil
}

func applyMaterializationShards(ctx context.Context, built *bindings.BuiltCatalog,
	client pc.ShardClient, shards int, commonsRevision int64) error {
	for _, spec := range built.Materializations {
		if err := createTaskShards(ctx, client,
			flowLabels.TaskTypeMaterialization,
			spec.Materialization.String(),
			shards,
			commonsRevision,
		); err != nil {
			return err
		}
	}
	return nil
}

func createTaskShards(ctx context.Context, client pc.ShardClient,
	taskType, taskName string, shards int, taskCreated int64) error {
	// Query for existing shards of this catalog task.
	if resp, err := consumer.ListShards(ctx, client, &pc.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet(
			flowLabels.TaskType, taskType,
			flowLabels.TaskName, taskName,
		)},
	}); err != nil {
		return fmt.Errorf("listing shard: %w", err)
	} else if len(resp.Shards) != 0 {
		log.WithField("task", taskName).Debug("shards exist")
		return nil
	}

	log.WithField("task", taskName).Debug("shard doesn't exist (will create)")

	var changes []pc.ApplyRequest_Change
	for p := 0; p != shards; p++ {

		var labels = pb.MustLabelSet(
			labels.ManagedBy, flowLabels.ManagedByFlow,
			flowLabels.TaskName, taskName,
			flowLabels.TaskType, taskType,
			flowLabels.TaskCreated, fmt.Sprintf("%d", taskCreated),
		)

		labels = flowLabels.EncodeRange(pf.RangeSpec{
			KeyBegin:    uint32((1 << 32) * (p + 0) / shards),
			KeyEnd:      uint32(((1 << 32) * (p + 1) / shards) - 1),
			RClockBegin: 0,
			RClockEnd:   math.MaxUint32,
		}, labels)

		var id, err = flowLabels.BuildShardID(labels)
		if err != nil {
			return fmt.Errorf("building shard ID: %w", err)
		}

		changes = append(changes, pc.ApplyRequest_Change{
			Upsert: &pc.ShardSpec{
				Id:                id,
				Sources:           nil,
				RecoveryLogPrefix: "recovery",
				HintPrefix:        "/estuary/flow/hints",
				HintBackups:       2,
				MaxTxnDuration:    time.Minute,
				MinTxnDuration:    0,
				HotStandbys:       0,
				LabelSet:          labels,
			},
			ExpectModRevision: 0, // Apply fails if it exists.
		})
	}

	var _, err = consumer.ApplyShards(ctx,
		client, &pc.ApplyRequest{Changes: changes})
	return err
}

func applyMaterializations(ctx context.Context, built *bindings.BuiltCatalog, dryRun bool) error {
	for _, spec := range built.Materializations {
		driver, err := materialize.NewDriver(ctx,
			spec.EndpointType, json.RawMessage(spec.EndpointSpecJson), "")
		if err != nil {
			return fmt.Errorf("building driver for materialization %q: %w", spec.Materialization, err)
		}

		response, err := driver.Apply(ctx, &pm.ApplyRequest{
			Materialization: &spec,
			Version:         built.ID.String(), // Use catalog commons ID.
			DryRun:          dryRun,
		})
		if err != nil {
			return fmt.Errorf("applying materialization %q: %w", spec.Materialization, err)
		}

		if response.ActionDescription != "" {
			fmt.Println("Applying materialization ", spec.Materialization, ":")
			fmt.Println(response.ActionDescription)
		}
	}
	return nil
}
