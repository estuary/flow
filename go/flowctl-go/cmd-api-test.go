package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/testing"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type apiTest struct {
	Broker      mbp.ClientConfig      `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	BuildID     string                `long:"build-id" required:"true" description:"ID of this build"`
	Consumer    mbp.ClientConfig      `group:"Consumer" namespace:"consumer" env-namespace:"CONSUMER"`
	Snapshot    string                `long:"snapshot" description:"When set, failed test verifications produce snapshots into the given base directory"`
	Log         mbp.LogConfig         `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Diagnostics mbp.DiagnosticsConfig `group:"Debug" namespace:"debug" env-namespace:"DEBUG"`
}

func (cmd apiTest) execute(ctx context.Context) error {
	ctx = pb.WithDispatchDefault(ctx)

	rjc, _, err := newJournalClient(ctx, cmd.Broker)
	if err != nil {
		return err
	}
	sc, _, err := newShardClient(ctx, cmd.Consumer)
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
	var tc = pf.NewTestingClient(cmd.Consumer.MustDial(ctx))

	var build = builds.Open(cmd.BuildID)
	defer build.Close()

	// Identify tests to verify and associated collections & schemas.
	var config pf.BuildAPI_Config
	var collections []*pf.CollectionSpec
	var derivations []*pf.DerivationSpec
	var tests []*pf.TestSpec

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
		return nil
	}); err != nil {
		return fmt.Errorf("extracting from build: %w", err)
	}

	// Run all test cases ordered by their scope, which implicitly orders on resource file and test name.
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Steps[0].StepScope < tests[j].Steps[0].StepScope
	})

	// Build a testing graph and driver to track and drive test execution.
	driver, err := testing.NewClusterDriver(ctx, sc, rjc, tc, cmd.BuildID, collections)
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

		if scope, err := testing.RunTestCase(ctx, graph, driver, testCase); err != nil {
			var path, ptr = scopeToPathAndPtr(config.Directory, scope)
			fmt.Println("❌", yellow(path), "failure at step", red(ptr), ":")
			fmt.Println(err)
			failed = append(failed, testCase.Test)

			var verify testing.FailedVerifies
			if errors.As(err, &verify) {
				if err := cmd.snapshot(verify); err != nil {
					return fmt.Errorf("creating snapshot: %w", err)
				}
			}
		} else {
			var path, _ = scopeToPathAndPtr(config.Directory, testCase.Steps[0].StepScope)
			fmt.Println("✔️", path, "::", green(testCase.Test))
		}

		var _, err = tc.ResetState(ctx, &pf.ResetStateRequest{})
		if err != nil {
			return fmt.Errorf("resetting internal state between test cases: %w", err)
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
	var ctx, cancelFn = context.WithTimeout(context.Background(), time.Minute)
	defer cancelFn()

	log.WithFields(log.Fields{
		"config":    cmd,
		"version":   mbp.Version,
		"buildDate": mbp.BuildDate,
	}).Info("flowctl configuration")
	pb.RegisterGRPCDispatcher("local")

	return cmd.execute(ctx)
}

func (cmd apiTest) snapshot(verify testing.FailedVerifies) error {
	if cmd.Snapshot == "" {
		return nil
	}

	var dir = filepath.Join(cmd.Snapshot, verify.Test.Test)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	var filename = filepath.Join(dir, fmt.Sprintf("verify-%d.json", verify.TestStep))
	var snap, err = os.Create(filename)
	if err != nil {
		return err
	}
	defer snap.Close()

	var enc = json.NewEncoder(snap)
	enc.SetIndent("", "  ")

	if err := enc.Encode(verify.Actuals); err != nil {
		return err
	}

	log.WithField("path", filename).Warn("wrote snapshot")
	return nil
}
