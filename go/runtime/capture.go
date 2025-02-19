package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/estuary/flow/go/shuffle"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// captureApp is a top-level Application which implements the capture workflow.
type captureApp struct {
	*taskBase[*pf.CaptureSpec]
	client       pc.Connector_CaptureClient
	isRestart    bool                  // Marks the current consumer transaction is a restart.
	pollCh       chan pf.OpFuture      // Coordinates polls of the client.
	restarts     message.Clock         // Increments for each restart.
	transactions message.Clock         // Increments for each transaction.
	watches      []*client.WatchedList // Watches of binding journals.
	watchCancel  context.CancelFunc    // Canceler of watches.
}

var _ application = (*captureApp)(nil)

// newCaptureApp returns a new Capture, which implements Application.
func newCaptureApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*captureApp, error) {
	var base, err = newTaskBase[*pf.CaptureSpec](host, shard, recorder, extractCaptureSpec)
	if err != nil {
		return nil, err
	}
	go base.heartbeatLoop(shard)

	client, err := pc.NewConnectorClient(base.svc.Conn()).Capture(shard.Context())
	if err != nil {
		base.drop()
		return nil, fmt.Errorf("starting capture stream: %w", err)
	}

	var pollCh = make(chan pf.OpFuture, 1)
	pollCh <- pf.FinishedOperation(nil)

	return &captureApp{
		taskBase:     base,
		client:       client,
		isRestart:    false,
		pollCh:       pollCh,
		restarts:     0,
		transactions: 0,
	}, nil
}

// RestoreCheckpoint initializes a catalog task term and restores the last
// persisted checkpoint, if any, by delegating to its JsonStore.
func (c *captureApp) RestoreCheckpoint(shard consumer.Shard) (_ pf.Checkpoint, _err error) {
	defer func() {
		if _err != nil {
			c.term.cancel()
		}
	}()
	if err := c.initTerm(shard); err != nil {
		return pf.Checkpoint{}, err
	}

	var watchCtx context.Context
	c.watches = c.watches[:0] // Truncate.

	if c.watchCancel != nil {
		c.watchCancel() // Cancel watches of previous term.
	}
	watchCtx, c.watchCancel = context.WithCancel(shard.Context())

	for _, binding := range c.term.taskSpec.Bindings {
		c.watches = append(c.watches, client.NewWatchedList(
			watchCtx,
			shard.JournalClient(),
			flow.CollectionWatchRequest(&binding.Collection),
			nil,
		))
	}
	// Wait for all watches to perform their first update.
	for _, watch := range c.watches {
		select {
		case err := <-watch.UpdateCh():
			if err != nil {
				return pf.Checkpoint{}, fmt.Errorf("initializing journal watch: %w", err)
			}
		case <-c.term.ctx.Done():
			return pf.Checkpoint{}, c.term.ctx.Err()
		}
	}

	var requestExt = &pr.CaptureRequestExt{
		LogLevel: c.term.labels.LogLevel,
	}
	if c.termCount == 1 {
		requestExt.RocksdbDescriptor = bindings.NewRocksDBDescriptor(c.recorder)
	}

	// Send Apply / receive Applied.
	_ = doSend[pc.Response](c.client, &pc.Request{
		Apply: &pc.Request_Apply{
			Capture: c.term.taskSpec,
			Version: c.term.labels.Build,
		},
		Internal: pr.ToInternal(requestExt),
	})
	if _, err := doRecv[pc.Response](c.client); err != nil {
		return pf.Checkpoint{}, err
	}

	// Send Open / receive Opened.
	_ = doSend[pc.Response](c.client, &pc.Request{
		Open: &pc.Request_Open{
			Capture:   c.term.taskSpec,
			Version:   c.term.labels.Build,
			Range:     &c.term.labels.Range,
			StateJson: json.RawMessage("{}"),
		},
		Internal: pr.ToInternal(&pr.CaptureRequestExt{LogLevel: c.term.labels.LogLevel}),
	})

	var opened, err = doRecv[pc.Response](c.client)
	if err != nil {
		return pf.Checkpoint{}, err
	}
	var openedExt = pr.FromInternal[pr.CaptureResponseExt](opened.Internal)
	c.container.Store(openedExt.Container)

	return *openedExt.Opened.RuntimeCheckpoint, nil
}

// StartReadingMessages starts a concurrent read of the pull RPC,
// which notifies into the consumer channel as data becomes available.
func (c *captureApp) StartReadingMessages(
	shard consumer.Shard,
	cp pf.Checkpoint,
	_ *flow.Timepoint,
	ch chan<- consumer.EnvelopeOrError,
) {
	// A consumer.Envelope requires a JournalSpec, of which only the Name is actually
	// used (for sequencing messages and producing checkpoints).
	// Of course, captures don't actually have a journal from which they read,
	// so invent minimal JournalSpecs which interoperate with the `consumer`
	// package. These pseudo-specs model connector transactions and restarts.
	//
	// In the future, we *may* want to generalize the `consumer` package to decouple
	// its current tight binding with JournalSpecs.

	var txnJournal = &pf.JournalSpec{Name: pf.Journal(fmt.Sprintf("%s/txn", c.term.taskSpec.Name))}
	var eofJournal = &pf.JournalSpec{Name: pf.Journal(fmt.Sprintf("%s/eof", c.term.taskSpec.Name))}

	go pollLoop(
		ch,
		c.client,
		&c.container,
		eofJournal, txnJournal,
		c.pollCh,
		&c.restarts, &c.transactions,
		shard.Context(), c.term.ctx,
	)
}

func pollLoop(
	ch chan<- consumer.EnvelopeOrError,
	client pc.Connector_CaptureClient,
	container *atomic.Pointer[pr.Container],
	eofJournal, txnJournal *pf.JournalSpec,
	pollCh chan pf.OpFuture,
	restarts, transactions *message.Clock, // Exclusively owned while running.
	shardCtx, termCtx context.Context,
) (__err error) {

	// On return, surface terminal error and then close `ch`.
	defer func() {
		if __err != nil {
			ch <- consumer.EnvelopeOrError{Error: __err}
		}
		close(ch)
	}()

	// Messages that a capture shard "reads" are really just notifications that
	// data is ready, and that it should run a consumer transaction to publish
	// the pre-combined documents and driver checkpoint.
	//
	// The concepts of a message.Clock and journal offset don't have much meaning,
	// since there *is* no journal and we're not reading timestamped messages.
	// So, use a single monotonic counter for both the message.Clock and pseudo-
	// journal offsets that ticks upwards by one with each "read" message.

	for {
		// Wait for cancellation or the next polling token.
		var op pf.OpFuture

		select {
		case <-termCtx.Done():
			var err = termCtx.Err()

			// Is the term context cancelled, but the shard context is not?
			if err == context.Canceled && shardCtx.Err() == nil {
				// Term contexts are cancelled if the task's ShardSpec changes.
				// This is not a terminal error of the shard, and closing |ch|
				// will begin a new task term under the updated specification.
				err = nil
			}
			return err

		case op = <-pollCh:
		}

		// Wait for the prior commit's OpFuture to resolve successfully.
		if err := op.Err(); err != nil {
			return err
		}

		if err := doSend[pc.Response](client, &pc.Request{
			Acknowledge: &pc.Request_Acknowledge{Checkpoints: 0},
		}); err != nil {
			return err
		}
		polled, err := doRecv[pc.Response](client)
		if err != nil {
			return err
		}
		var polledExt = pr.FromInternal[pr.CaptureResponseExt](polled.Internal)

		switch polledExt.Checkpoint.PollResult {
		case pr.CaptureResponseExt_NOT_READY:
			pollCh <- op // Yield the polling token for next attempt.

		case pr.CaptureResponseExt_COOL_OFF:
			container.Store(nil) // Connector is no longer running.
			pollCh <- op

		case pr.CaptureResponseExt_READY:
			transactions.Tick()

			// Write one message which will start a Gazette consumer transaction.
			// We'll see a future a call to ConsumeMessage and then StartCommit.
			ch <- consumer.EnvelopeOrError{
				Envelope: message.Envelope{
					Journal: txnJournal,
					Begin:   int64(*transactions),
					End:     int64(*transactions + 1),
					Message: &captureMessage{clock: *transactions},
				},
			}

		case pr.CaptureResponseExt_RESTART:
			restarts.Tick()

			// Emit a no-op message, whose purpose is only to update the tracked EOF
			// offset, which may in turn unblock an associated shard Stat RPC.
			ch <- consumer.EnvelopeOrError{
				Envelope: message.Envelope{
					Journal: eofJournal,
					Begin:   int64(*restarts),
					End:     int64(*restarts + 1),
					Message: &captureMessage{
						clock:   *restarts,
						restart: true,
					},
				},
			}
			// Return to start a new task term.
			return nil

		default:
			return fmt.Errorf("invalid poll result: %#v", polledExt)
		}
	}
}

// ReplayRange is not valid for a Capture and must not be called.
func (c *captureApp) ReplayRange(_ consumer.Shard, _ pf.Journal, begin, end pf.Offset) message.Iterator {
	panic("ReplayRange is not valid for Capture runtime, and should never be called")
}

// ReadThrough returns its `offsets` unmodified.
func (c *captureApp) ReadThrough(offsets pf.Offsets) (pf.Offsets, error) {
	return offsets, nil
}

// ConsumeMessage drains the capture transaction,
// and publishes each document to its captured collection.
func (c *captureApp) ConsumeMessage(shard consumer.Shard, env message.Envelope, pub *message.Publisher) error {
	if env.Message.(*captureMessage).restart {
		c.isRestart = true // This is not a transaction notification.
		return nil
	}
	var mapper = flow.NewMapper(shard.Context(), shard.JournalClient())
	var stats *ops.Stats

	// Transaction responses are completed with a final checkpoint that has stats.
	// Preceding checkpoints have state updates, which we don't care about here.
	for stats == nil {
		var response, err = doRecv[pc.Response](c.client)
		if err != nil {
			return err
		}
		var responseExt = pr.FromInternal[pr.CaptureResponseExt](response.Internal)

		if response.Captured != nil {
			var captured = response.Captured
			var capturedExt = responseExt.Captured

			partitions, err := tuple.Unpack(capturedExt.PartitionsPacked)
			if err != nil {
				return fmt.Errorf("unpacking partitions: %w", err)
			}
			if _, err = pub.PublishUncommitted(mapper.Map, flow.Mappable{
				Spec:       &c.term.taskSpec.Bindings[captured.Binding].Collection,
				Doc:        captured.DocJson,
				PackedKey:  capturedExt.KeyPacked,
				Partitions: partitions,
				List:       c.watches[captured.Binding],
			}); err != nil {
				return fmt.Errorf("publishing document: %w", err)
			}
		} else if response.Checkpoint != nil {
			if responseExt.Checkpoint != nil {
				stats = responseExt.Checkpoint.Stats
			}
		} else {
			return fmt.Errorf("expected Captured or Checkpoint, but got %#v", response)
		}
	}

	if len(stats.Capture) == 0 {
		// The connector may have only emitted an empty checkpoint.
		// Don't publish stats in this case.
		ops.PublishLog(c.opsPublisher, ops.Log_debug,
			"capture transaction committing updating driver checkpoint only")
	} else if err := c.opsPublisher.PublishStats(*stats, pub.PublishUncommitted); err != nil {
		return fmt.Errorf("publishing stats: %w", err)
	}

	return nil
}

func (c *captureApp) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	if c.isRestart {
		c.isRestart = false
		return pf.FinishedOperation(nil)
	}

	ops.PublishLog(c.opsPublisher, ops.Log_debug,
		"StartCommit",
		"capture", c.term.labels.TaskName,
		"shard", c.term.shardSpec.Id,
		"build", c.term.labels.Build,
	)

	// Install a barrier such that we don't begin writing until `waitFor` has resolved.
	_ = c.recorder.Barrier(waitFor)

	// Tell capture runtime we're starting to commit.
	if err := doSend[pc.Response](c.client, &pc.Request{
		Internal: pr.ToInternal(&pr.CaptureRequestExt{
			StartCommit: &pr.CaptureRequestExt_StartCommit{RuntimeCheckpoint: &cp},
		}),
	}); err != nil {
		return client.FinishedOperation(err)
	}
	// Await it's StartedCommit, which tells us that all recovery log writes have been sequenced.
	if started, err := doRecv[pc.Response](c.client); err != nil {
		return client.FinishedOperation(err)
	} else if started.Checkpoint == nil { // Checkpoint is used for StartedCommit.
		return client.FinishedOperation(fmt.Errorf("expected StartedCommit, but got %#v", started))
	}

	// Another barrier which notifies when the WriteBatch
	// has been durably recorded to the recovery log.
	return c.recorder.Barrier(nil)
}

func (c *captureApp) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {
	c.pollCh <- op // Yield transaction's commit future as the next polling token.
}

func (c *captureApp) Destroy() {
	if c.client != nil {
		_ = c.client.CloseSend()
	}
	c.taskBase.drop()
	c.taskBase.opsCancel()
}

func (c *captureApp) BeginTxn(consumer.Shard) error                                  { return nil } // No-op.
func (c *captureApp) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error { return nil } // No-op.

// Coordinator panics if called.
func (c *captureApp) Coordinator() *shuffle.Coordinator {
	panic("Coordinator is not valid for Capture runtime, and should never be called")
}

type captureMessage struct {
	clock   message.Clock // Monotonic Clock counting capture transactions and exits.
	restart bool          // True if the connector exited.
}

func (m *captureMessage) GetUUID() message.UUID {
	return message.BuildUUID(message.ProducerID{}, m.clock, message.Flag_OUTSIDE_TXN)
}
func (m *captureMessage) SetUUID(message.UUID) {
	panic("must not be called")
}
func (m *captureMessage) NewAcknowledgement(pf.Journal) message.Message {
	panic("must not be called")
}

func extractCaptureSpec(db *sql.DB, taskName string) (*pf.CaptureSpec, error) {
	return catalog.LoadCapture(db, taskName)
}
