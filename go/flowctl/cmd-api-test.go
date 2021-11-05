package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/testing"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiTest struct {
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID     string                `long:"build-id" required:"true" description:"ID of this build"`
	BuildsRoot  string                `long:"builds-root" required:"true" env:"BUILDS_ROOT" description:"Base URL for fetching Flow catalog builds"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiTest) execute(ctx context.Context) error {
	var builds, err = flow.NewBuildService(cmd.BuildsRoot)
	if err != nil {
		return err
	}

	ctx = pb.WithDispatchDefault(ctx)
	var sc = cmd.Consumer.MustShardClient(ctx)
	var tc = pf.NewTestingClient(cmd.Consumer.MustDial(ctx))
	var rjc = cmd.Broker.MustRoutedJournalClient(ctx)

	// Ping to ensure connectivity.
	if err = pingClients(ctx, sc, rjc); err != nil {
		return err
	}

	var build = builds.Open(cmd.BuildID)
	defer build.Close()

	// Identify tests to verify and associated collections & schemas.
	var config pf.BuildAPI_Config
	var collections []*pf.CollectionSpec
	var derivations []*pf.DerivationSpec
	var tests []*pf.TestSpec
	var bundle pf.SchemaBundle

	if err := build.Extract(func(db *sql.DB) error {
		if config, err = catalog.LoadBuildConfig(db); err != nil {
			return err
		}
		if collections, err = catalog.LoadAllCollections(db); err != nil {
			return err
		}
		if derivations, err = catalog.LoadAllDerivations(db); err != nil {
			return err
		}
		if tests, err = catalog.LoadAllTests(db); err != nil {
			return err
		}
		if bundle, err = catalog.LoadSchemaBundle(db); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	// Run all test cases ordered by their scope, which implicitly orders on resource file and test name.
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Steps[0].StepScope < tests[j].Steps[0].StepScope
	})

	// Build a testing graph and driver to track and drive test execution.
	driver, err := testing.NewClusterDriver(ctx, sc, rjc, tc, cmd.BuildID, &bundle, collections)
	if err != nil {
		return fmt.Errorf("building test driver: %w", err)
	}

	var graph = testing.NewGraph(nil, derivations, nil)
	if err = testing.Initialize(ctx, driver, graph); err != nil {
		return fmt.Errorf("initializing dataflow tracking: %w", err)
	}

	var failed []string
	fmt.Println("Running ", len(tests), " tests...")

	for _, testCase := range tests {
		if ctx.Err() != nil {
			break
		}

		var _, err = tc.ResetState(ctx, &pf.ResetStateRequest{})
		if err != nil {
			return fmt.Errorf("reseting internal state between test cases: %w", err)
		} else if scope, err := testing.RunTestCase(ctx, graph, driver, testCase); err != nil {
			var path, ptr = scopeToPathAndPtr(config.Directory, scope)
			fmt.Println("❌", yellow(path), "failure at step", red(ptr), ":")
			fmt.Println(err)
			failed = append(failed, testCase.Test)
		} else {
			var path, _ = scopeToPathAndPtr(config.Directory, testCase.Steps[0].StepScope)
			fmt.Println("✔️", path, "::", green(testCase.Test))
		}
	}

	fmt.Printf("\nRan %d tests, %d passed, %d failed\n",
		len(tests), len(tests)-len(failed), len(failed))

	if failed != nil {
		return fmt.Errorf("failed tests: [%s]", strings.Join(failed, ", "))
	}

	if err := build.Close(); err != nil {
		return fmt.Errorf("closing build: %w", err)
	}
	return nil
}

func (cmd apiTest) Execute(_ []string) error {
	defer mbp.InitDiagnosticsAndRecover(cmd.Diagnostics)()
	mbp.InitLog(cmd.Log)

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	return cmd.execute(context.Background())
}
