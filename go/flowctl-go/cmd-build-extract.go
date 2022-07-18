package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiBuildExtract struct {
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID     string                `long:"build-id" required:"true" description:"ID of this build"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiBuildExtract) execute(ctx context.Context) error {
	ctx = pb.WithDispatchDefault(ctx)

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

	// Load collections and tasks.
	var collections []*pf.CollectionSpec
	var captures []*pf.CaptureSpec
	var derivations []*pf.DerivationSpec
	var materializations []*pf.MaterializationSpec

	if err := build.Extract(func(db *sql.DB) error {
		if collections, err = catalog.LoadAllCollections(db); err != nil {
			return err
		}
		if captures, err = catalog.LoadAllCaptures(db); err != nil {
			return err
		}
		if derivations, err = catalog.LoadAllDerivations(db); err != nil {
			return err
		}
		if materializations, err = catalog.LoadAllMaterializations(db); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	os.Stdout.Write([]byte{byte(len(captures))})
	for _, capture := range captures {
		byts, err := proto.Marshal(capture)
		if err != nil {
			return err
		}
		os.Stdout.Write(byts)
	}

	os.Stdout.Write([]byte{byte(len(collections))})
	for _, collection := range collections {
		byts, err := proto.Marshal(collection)
		if err != nil {
			return err
		}
		os.Stdout.Write(byts)
	}

	os.Stdout.Write([]byte{byte(len(derivations))})
	for _, derivation := range derivations {
		byts, err := proto.Marshal(derivation)
		if err != nil {
			return err
		}
		os.Stdout.Write(byts)
	}

	OutputMessages(materializations)
	os.Stdout.Write([]byte{byte(len(materializations))})
	for _, materialization := range materializations {
		byts, err := proto.Marshal(materialization)
		if err != nil {
			return err
		}
		os.Stdout.Write(byts)
	}

	if err := build.Close(); err != nil {
		return fmt.Errorf("closing build: %w", err)
	}
	return nil
}

/*func OutputMessages(messages []*proto.Message) error {
	os.Stdout.Write([]byte{byte(len(messages))})
	for _, message := range messages {
		byts, err := proto.Marshal(*message)
		if err != nil {
			return err
		}
		os.Stdout.Write(byts)
	}
}*/

func (cmd apiBuildExtract) Execute(_ []string) error {
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
