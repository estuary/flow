package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	flowLabels "github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize/driver"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/runtime"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
	"google.golang.org/grpc"
)

type cmdApply struct {
	Source      string                `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Directory   string                `long:"directory" default:"." description:"Build directory"`
	DryRun      bool                  `long:"dry-run" description:"Dry run, don't actually apply"`
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

	built, err := buildCatalog(pf.BuildAPI_Config{
		CatalogPath:       filepath.Join(cmd.Directory, "catalog.db"),
		Directory:         cmd.Directory,
		Source:            cmd.Source,
		TypescriptPackage: true,
	})
	if err != nil {
		return err
	} else if len(built.NPMPackage) == 0 {
		panic("built with TypescriptPackage: true but NPM package is empty")
	}

	var ctx = context.Background()
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

	// Apply all database materializations first, before we create
	// catalog entities that reference the applied tables / topics / targets.
	if err := applyMaterializationsTODO(built, cmd.DryRun); err != nil {
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
	if _, _, err := flow.ApplyCatalogToEtcd(flow.ApplyArgs{
		Ctx:                  ctx,
		Etcd:                 etcd,
		Root:                 cmd.Flow.CatalogRoot,
		Build:                built,
		TypeScriptUDS:        "",
		TypeScriptPackageURL: "etcd://" + packageKey,
		DryRun:               cmd.DryRun,
	}); err != nil {
		return fmt.Errorf("applying catalog to Etcd: %w", err)
	}

	if !cmd.DryRun {
		// Apply derivation shard specs.
		if err = applyDerivationShardsTODO(built, shards); err != nil {
			return fmt.Errorf("applying derivation shards: %w", err)
		}
		// Apply materialization shards.
		if err = applyMaterializationShardsTODO(built, shards); err != nil {
			return fmt.Errorf("applying materialization shards: %w", err)
		}
	}

	return err
}

func buildCatalog(config pf.BuildAPI_Config) (*bindings.BuiltCatalog, error) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		BuildAPI_Config:     config,
		FileRoot:            "/",
		MaterializeDriverFn: driver.NewDriver,
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
	return path, ptr
}

func applyDerivationShardsTODO(built *bindings.BuiltCatalog, shards pc.ShardClient) error {
	for _, spec := range built.Derivations {
		var name = spec.Collection.Collection.String()
		var id = pc.ShardID(fmt.Sprintf("derivation/%s/%s-%s",
			name, flowLabels.KeyBeginMin, flowLabels.RClockBeginMin))

		var labels = pb.MustLabelSet(
			labels.ManagedBy, flowLabels.ManagedByFlow,
			flowLabels.TaskName, name,
			flowLabels.TaskType, flowLabels.TaskTypeDerivation,
			flowLabels.KeyBegin, flowLabels.KeyBeginMin,
			flowLabels.KeyEnd, flowLabels.KeyEndMax,
			flowLabels.RClockBegin, flowLabels.RClockBeginMin,
			flowLabels.RClockEnd, flowLabels.RClockEndMax,
		)

		var changes = []pc.ApplyRequest_Change{{
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
		}}

		var _, err = consumer.ApplyShards(context.Background(),
			shards, &pc.ApplyRequest{Changes: changes})

		if err == nil {
			log.WithField("id", id).Debug("created derivation shard")
		} else if err.Error() == pc.Status_ETCD_TRANSACTION_FAILED.String() {
			log.WithField("id", id).Debug("derivation shard exists")
		} else {
			return fmt.Errorf("applying shard %q: %w", changes[0].Upsert.Id, err)
		}
	}
	return nil
}

func applyMaterializationShardsTODO(built *bindings.BuiltCatalog, shards pc.ShardClient) error {
	for _, spec := range built.Materializations {
		var name = spec.Materialization
		var id = pc.ShardID(fmt.Sprintf("materialize/%s/%s-%s",
			name, flowLabels.KeyBeginMin, flowLabels.RClockBeginMin))

		var labels = pb.MustLabelSet(
			labels.ManagedBy, flowLabels.ManagedByFlow,
			flowLabels.TaskName, name,
			flowLabels.TaskType, flowLabels.TaskTypeMaterialization,
			flowLabels.KeyBegin, flowLabels.KeyBeginMin,
			flowLabels.KeyEnd, flowLabels.KeyEndMax,
			flowLabels.RClockBegin, flowLabels.RClockBeginMin,
			flowLabels.RClockEnd, flowLabels.RClockEndMax,
		)

		var changes = []pc.ApplyRequest_Change{{
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
		}}

		var _, err = consumer.ApplyShards(context.Background(), shards,
			&pc.ApplyRequest{Changes: changes})

		if err == nil {
			log.WithField("id", id).Debug("created materialization shard")
		} else if err.Error() == pc.Status_ETCD_TRANSACTION_FAILED.String() {
			log.WithField("id", id).Debug("materialization shard exists")
		} else {
			return fmt.Errorf("applying shard %q: %w", changes[0].Upsert.Id, err)
		}
	}
	return nil
}

func applyMaterializationsTODO(built *bindings.BuiltCatalog, dryRun bool) error {
	for _, spec := range built.Materializations {
		driver, err := driver.NewDriver(context.Background(),
			spec.EndpointType, json.RawMessage(spec.EndpointConfigJson), "")
		if err != nil {
			return fmt.Errorf("building driver for materialization: %w", err)
		}

		response, err := driver.Apply(context.Background(), &pm.ApplyRequest{
			Materialization: &spec,
			DryRun:          dryRun,
		})
		if err != nil {
			return fmt.Errorf("applying materialization: %w", err)
		}

		if response.ActionDescription != "" {
			fmt.Println("Applying materialization ", spec.Materialization, ":")
			fmt.Println(response.ActionDescription)
		}
	}
	return nil
}
