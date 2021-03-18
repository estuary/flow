package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/estuary/flow/go/bindings"
	flowLabels "github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize/driver"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdBuild struct {
	Source    string `long:"source" required:"true" description:"Catalog source file or URL to build"`
	Directory string `long:"directory" default:"." description:"Build directory"`
}

func (cmd cmdBuild) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(Config.Diagnostics)()
	initLog(Config.Log)

	log.WithFields(log.Fields{
		"config":    Config,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")

	var config = pf.BuildAPI_Config{
		Source:            cmd.Source,
		Directory:         cmd.Directory,
		CatalogPath:       filepath.Join(cmd.Directory, "catalog.db"),
		TypescriptCompile: true,
		TypescriptPackage: true,
	}

	var _, err = buildCatalog(config)
	if err == nil {
		fmt.Println("Build Success")
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
		log.WithField("scope", be.Scope).Error(be.Error)
	}
	if len(built.Errors) != 0 {
		return nil, fmt.Errorf("one or more catalog errors")
	}
	return built, nil
}

func applyDerivationShardsTODO(built *bindings.BuiltCatalog, shards pc.ShardClient) error {
	for _, spec := range built.Derivations {
		var name = spec.Collection.Collection.String()
		var id = pc.ShardID(fmt.Sprintf("derivation/%s/%s-%s",
			name, flowLabels.KeyBeginMin, flowLabels.RClockBeginMin))

		var labels = pb.MustLabelSet(
			labels.ManagedBy, flowLabels.ManagedByFlow,
			flowLabels.CatalogTask, name,
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
			flowLabels.CatalogTask, name,
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

		fmt.Println(response.ActionDescription)
	}
	return nil
}
