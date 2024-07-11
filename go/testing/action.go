package testing

import (
	"context"
	"errors"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// Driver executes test actions.
type Driver interface {
	// Apply one or more PendingStats of shards, which can now be expected to complete.
	// `readThrough` journals have a shuffle suffix, while `writeAt` journals do not.
	Stat(context.Context, PendingStat) (readThrough pb.Offsets, writeAt pb.Offsets, _ error)
	// Execute an "Ingest" TestSpec_Step.
	// `writeAt` journals do not have a shuffle suffix.
	Ingest(_ context.Context, _ *pf.TestSpec, testStep int) (writeAt pb.Offsets, _ error)
	// Execute a "Verify" TestStep.
	// `from` and `to` journals do not have a shuffle suffix.
	Verify(_ context.Context, _ *pf.TestSpec, testStep int, from, to pb.Offsets) error
	// Advance TestTime by the given delta.
	Advance(context.Context, TestTime) error
}

// ErrAdvanceDisabled is a specific error returned when clock advance is called when advance is disabled.
var ErrAdvanceDisabled = errors.New("advance disabled")

// RunTestCase runs a test case using the given Graph and Driver.
func RunTestCase(ctx context.Context, graph *Graph, driver Driver, test *pf.TestSpec) (scope string, err error) {
	var initial = graph.writeClock.Copy()
	var testStep = 0

	for {
		var ready, nextReady, nextName = graph.PopReadyStats()

		for _, stat := range ready {
			var read, write, err = driver.Stat(ctx, stat)
			if err != nil {
				return scope, fmt.Errorf("driver.Stat: %w", err)
			}
			graph.CompletedStat(stat.TaskName, read, write)
		}

		// If we completed stats, loop again to look for more ready stats.
		if len(ready) != 0 {
			continue
		}

		var step *pf.TestSpec_Step
		if testStep != len(test.Steps) {
			step = &test.Steps[testStep]
			scope = step.StepScope
		}

		// Ingest test steps always run immediately.
		if step != nil && step.StepType == pf.TestSpec_Step_INGEST {
			var write, err = driver.Ingest(ctx, test, testStep)
			if err != nil {
				return scope, fmt.Errorf("ingest: %w", err)
			}
			graph.CompletedIngest(step.Collection, write)
			testStep++
			continue
		}

		// Verify steps may run only if no dependent PendingStats remain.
		if step != nil && step.StepType == pf.TestSpec_Step_VERIFY &&
			!graph.HasPendingWrite(step.Collection) {

			if err := driver.Verify(ctx, test, testStep, initial, graph.writeClock); err != nil {
				return scope, fmt.Errorf("verify: %w", err)
			}
			testStep++
			continue
		}

		// Advance time to unblock the next PendingStat.
		if nextReady != -1 {
			if err := driver.Advance(ctx, nextReady); err == ErrAdvanceDisabled {
				log.WithFields(log.Fields{"delay": nextReady, "task": nextName}).
					Warn("task reads with a time delay and may block")
			} else if err != nil {
				return scope, fmt.Errorf("driver.Advance: %w", err)
			}
			graph.CompletedAdvance(nextReady)
			continue
		}

		// All steps are completed, and no pending stats remain. All done.
		if testStep != len(test.Steps) {
			panic("unexpected test steps remain")
		}
		return scope, nil
	}
}
