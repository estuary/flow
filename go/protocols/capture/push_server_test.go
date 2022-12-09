package capture_test

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
)

var localPublisher = ops.NewLocalPublisher(
	labels.ShardLabeling{
		Build:    "capture-test",
		LogLevel: pf.LogLevel_debug,
		Range: pf.RangeSpec{
			KeyBegin:    0x00001111,
			KeyEnd:      0x11110000,
			RClockBegin: 0x00002222,
			RClockEnd:   0x22220000,
		},
		TaskName: "capture-test/task/name",
		TaskType: labels.TaskTypeCapture,
	},
)

func TestPushServerLifecycle(t *testing.T) {
	var specBytes, err = ioutil.ReadFile("testdata/capture.proto")
	require.NoError(t, err)
	var spec pf.CaptureSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	var startCommitCh = make(chan error)

	var ctx, cancel = context.WithCancel(context.Background())
	push, err := capture.NewPushServer(
		ctx,
		func(*pf.CaptureSpec_Binding) (pf.Combiner, error) {
			return new(pf.MockCombiner), nil
		},
		func(binding *flow.CaptureSpec_Binding) (pf.Extractor, error) {
			return new(pf.MockExtractor), nil
		},
		pf.NewFullRange(),
		&spec,
		"a-version",
		func(err error) { startCommitCh <- err },
	)
	require.NoError(t, err)

	var captured []json.RawMessage
	var reducedCheckpoint pf.DriverCheckpoint

	// drain takes Combined documents from the MockCombiner, appending them into
	// |captured|, and reduces the driver checkpoint into |reducedCheckpoint|.
	var drain = func() {
		var combiners, checkpoint = push.PopTransaction()

		var combiner = combiners[0].(*pf.MockCombiner)
		captured = append(captured, combiner.Combined...)
		combiner.Combined = nil

		require.NoError(t, reducedCheckpoint.Reduce(checkpoint))
	}

	var acksOp1 = client.NewAsyncOperation()

	require.NoError(t, push.Push(
		[]capture.Documents{*makeDocs(0, "one"), *makeDocs(0, "two")},
		*makeCheckpoint(map[string]int{"a": 1}),
		acksOp1,
	))

	// Expect Serve notified our callback.
	require.NoError(t, <-startCommitCh)
	drain()

	// Tell Serve of a pending log commit.
	var commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))

	var acksOp2 = client.NewAsyncOperation()
	var acksOp3 = client.NewAsyncOperation()

	// Two new Pushes arrive.
	require.NoError(t, push.Push(
		[]capture.Documents{*makeDocs(0, "three")},
		*makeCheckpoint(map[string]int{"b": 1}),
		acksOp2,
	))
	require.NoError(t, push.Push(
		[]capture.Documents{*makeDocs(0, "four", "five")},
		*makeCheckpoint(map[string]int{"b": 2}),
		acksOp3,
	))

	commitOp.Resolve(nil)
	<-acksOp1.Done() // Expect first Push is acknowledged.

	// We were notified that the next commit is ready.
	require.NoError(t, <-startCommitCh)
	drain()

	commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	_, _ = <-acksOp2.Done(), <-acksOp2.Done() // Next two Pushes are acknowledged.

	// Lower the target threshold for combining push checkpoints,
	// so that the first and second pushes commit separately.
	defer func(i int) { capture.SetCombinerByteThreshold(i) }(capture.GetCombinerByteThreshold())
	capture.SetCombinerByteThreshold(1)

	var acksOp4 = client.NewAsyncOperation()
	var acksOp5 = client.NewAsyncOperation()

	// Next two pushes race our reads of the next ready commit.
	// However, we set a low combiner byte threshold, so we're guaranteed
	// that they commit separately (which would otherwise not be true).
	go func() {
		require.NoError(t, push.Push(
			[]capture.Documents{*makeDocs(0, "six", "seven")},
			*makeCheckpoint(map[string]int{"c": 1}),
			acksOp4,
		))
		// A checkpoint without Documents is also valid.
		require.NoError(t, push.Push(
			nil,
			*makeCheckpoint(map[string]int{"a": 2}),
			acksOp5,
		))

		// Begin a graceful top of Serve.
		cancel()
	}()

	// We are notified that two commits are ready.

	require.NoError(t, <-startCommitCh)
	drain()

	commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	<-acksOp4.Done() // Push is acknowledged.

	require.NoError(t, <-startCommitCh)
	drain()

	commitOp = client.NewAsyncOperation()
	require.NoError(t, push.SetLogCommitOp(commitOp))
	commitOp.Resolve(nil)
	<-acksOp5.Done() // Push is acknowledged.

	// Serve has stopped running.
	<-push.ServeOp().Done()

	// We're notified of the close.
	require.Equal(t, io.EOF, <-startCommitCh)
	// The client closes gracefully.
	require.NoError(t, push.Close())
	// A further attempt to push errors, since Serve is no longer listening.
	require.Equal(t, io.EOF, push.Push(nil, pf.DriverCheckpoint{}, client.NewAsyncOperation()))
	// A further attempt to set a LogCommitOp errors, since Serve is no longer listening.
	require.Equal(t, io.EOF, push.SetLogCommitOp(client.NewAsyncOperation()))

	// Snapshot the recorded observations of drains.
	cupaloy.SnapshotT(t,
		"DRIVER CHECKPOINT:", reducedCheckpoint,
		"CAPTURED", captured,
	)
}
