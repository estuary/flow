package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/ops"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Capture is a top-level Application which implements the capture workflow.
type Capture struct {
	driver *connector.Driver
	// delegate is a pc.PullClient or a pc.PushServer
	delegate delegate
	// delegateEOF is set after reading a delegate EOF.
	delegateEOF bool
	// FlowConsumer which owns this Capture shard.
	host *FlowConsumer
	// Specification under which the capture is currently running.
	spec pf.CaptureSpec
	// Store for persisting local checkpoints.
	store *consumer.JSONFileStore
	// Embedded processing state scoped to a current task version.
	// Updated in RestoreCheckpoint.
	taskTerm
}

var _ Application = (*Capture)(nil)

// NewCaptureApp returns a new Capture, which implements Application.
func NewCaptureApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Capture, error) {
	var store, err = newConnectorStore(recorder)
	if err != nil {
		return nil, fmt.Errorf("newConnectorStore: %w", err)
	}

	return &Capture{
		delegate:    nil,   // Initialized in RestoreCheckpoint.
		delegateEOF: false, // Initialized in RestoreCheckpoint.
		host:        host,
		spec:        pf.CaptureSpec{}, // Initialized in RestoreCheckpoint.
		store:       store,
		taskTerm:    taskTerm{}, // Initialized in RestoreCheckpoint.
	}, nil
}

// RestoreCheckpoint initializes a catalog task term and restores the last
// persisted checkpoint, if any, by delegating to its JsonStore.
func (c *Capture) RestoreCheckpoint(shard consumer.Shard) (cp pf.Checkpoint, err error) {
	if err = c.initTerm(shard, c.host); err != nil {
		return pf.Checkpoint{}, err
	}
	defer func() {
		if err == nil {
			ops.PublishLog(c.opsPublisher, pf.LogLevel_debug,
				"initialized processing term",
				"capture", c.labels.TaskName,
				"shard", c.shardSpec.Id,
				"build", c.labels.Build,
				"checkpoint", cp,
			)
		} else {
			ops.PublishLog(c.opsPublisher, pf.LogLevel_error,
				"failed to initialize processing term",
				"error", err,
			)
		}
	}()

	// Stop a previous Driver and PullClient / PushServer delegate if it exists.
	if c.delegate != nil {
		if err = c.delegate.Close(); err != nil && !errors.Is(err, context.Canceled) {
			return pf.Checkpoint{}, fmt.Errorf("closing previous connector client: %w", err)
		}
		c.delegate = nil
	}
	if c.driver != nil {
		if err = c.driver.Close(); err != nil && !errors.Is(err, context.Canceled) {
			return pf.Checkpoint{}, fmt.Errorf("closing previous connector driver: %w", err)
		}
		c.driver = nil
	}

	// Load the current term's CaptureSpec.
	err = c.build.Extract(func(db *sql.DB) error {
		captureSpec, err := catalog.LoadCapture(db, c.labels.TaskName)
		if captureSpec != nil {
			c.spec = *captureSpec
		}
		return err
	})
	if err != nil {
		return pf.Checkpoint{}, err
	}
	ops.PublishLog(c.opsPublisher, pf.LogLevel_debug,
		"loaded specification",
		"spec", c.spec, "build", c.labels.Build)

	if cp, err = c.store.RestoreCheckpoint(shard); err != nil {
		return pf.Checkpoint{}, err
	}

	return cp, nil
}

// StartReadingMessages starts a concurrent read of the pull RPC,
// which notifies into the consumer channel as data becomes available.
func (c *Capture) StartReadingMessages(
	shard consumer.Shard,
	cp pf.Checkpoint,
	_ *flow.Timepoint,
	ch chan<- consumer.EnvelopeOrError,
) {
	if err := c.startReadingMessages(shard, cp, ch); err != nil {
		ch <- consumer.EnvelopeOrError{Error: err}
	}
}

func (c *Capture) startReadingMessages(
	shard consumer.Shard,
	cp pf.Checkpoint,
	ch chan<- consumer.EnvelopeOrError,
) error {
	// A consumer.Envelope requires a JournalSpec, of which only the Name is actually
	// used (for sequencing messages and producing checkpoints).
	// Of course, captures don't actually have a journal from which they read,
	// so invent minimal JournalSpecs which interoperate with the `consumer`
	// package. These pseudo-specs model connector transactions and exits.
	//
	// In the future, we *may* want to generalize the `consumer` package to decouple
	// its current tight binding with JournalSpecs.

	var txnJournal = &pf.JournalSpec{Name: pf.Journal(fmt.Sprintf("%s/txn", c.spec.Capture))}
	var eofJournal = &pf.JournalSpec{Name: pf.Journal(fmt.Sprintf("%s/eof", c.spec.Capture))}

	// Messages that a capture shard "reads" are really just notifications that
	// data is ready, and that it should run a consumer transaction to publish
	// the pre-combined documents and driver checkpoint.
	//
	// The concepts of a message.Clock and journal offset don't have much meaning,
	// since there *is* no journal and we're not reading timestamped messages.
	// So, use a single monotonic counter for both the message.Clock and pseudo-
	// journal offsets that ticks upwards by one with each "read" message.
	// The counter is persisted in checkpoints and recovered across restarts.

	// Restore the largest Clock value previously recorded in the Checkpoint.
	var counter message.Clock
	for _, n := range []pf.Journal{txnJournal.Name, eofJournal.Name} {
		if c := message.Clock(cp.Sources[n].ReadThrough); c > counter {
			counter = c
		}
	}

	// Determine the minimum interval time of the connector.
	var minInterval = time.Duration(c.spec.IntervalSeconds) * time.Second
	var minTimer = time.NewTimer(minInterval)

	// startCommitFn is a closure which is called back when the client is ready
	// to commit documents and a corresponding driver checkpoint.
	var startCommitFn = func(err error) {
		counter.Tick()

		if err == nil {
			// Write one message which will start a Gazette consumer transaction.
			// We'll see a future a call to ConsumeMessage and then StartCommit.
			ch <- consumer.EnvelopeOrError{
				Envelope: message.Envelope{
					Journal: txnJournal,
					Begin:   int64(counter),
					End:     int64(counter + 1),
					Message: &captureMessage{clock: counter},
				},
			}
			return
		}

		// We've been notified of a terminal connector error.
		// We always close |ch| in response, but we:
		// * MAY wait for |minInterval| to elapse before doing so, OR
		// * MAY propagate an error into |ch| (terminally failing the shard).
		defer close(ch)

		// Is this is a graceful close of the capture?
		if err == io.EOF {
			// Emit a no-op message, whose purpose is only to update the tracked EOF
			// offset, which may in turn unblock an associated shard Stat RPC.
			ch <- consumer.EnvelopeOrError{
				Envelope: message.Envelope{
					Journal: eofJournal,
					Begin:   int64(counter),
					End:     int64(counter + 1),
					Message: &captureMessage{
						clock: counter,
						eof:   true,
					},
				},
			}

			// Wait for the minimum polling interval to elapse before closing,
			// which will drain the current task term and start another.
			// If we didn't wait, we would drive the connector in a hot loop.
			select {
			case <-minTimer.C:
				return
			case <-c.taskTerm.ctx.Done():
				err = c.taskTerm.ctx.Err()
				// Fallthrough.
			}
		}

		// Is the term context cancelled, but the shard context is not?
		if err == context.Canceled && shard.Context().Err() == nil {
			// Term contexts are cancelled if the task's ShardSpec changes.
			// This is not a terminal error of the shard, and closing |ch|
			// will begin a new task term under the updated specification.
			return
		}

		// Propagate all other errors as terminal.
		ch <- consumer.EnvelopeOrError{Error: err}
	}

	// Closure which builds a Combiner for a specified binding.
	var newCombinerFn = func(binding *pf.CaptureSpec_Binding) (pf.Combiner, error) {
		var combiner, err = bindings.NewCombine(c.opsPublisher)
		if err != nil {
			return nil, err
		}
		return combiner, combiner.Configure(
			shard.FQN(),
			binding.Collection.Collection,
			binding.Collection.SchemaJson,
			binding.Collection.UuidPtr,
			binding.Collection.KeyPtrs,
			flow.PartitionPointers(&binding.Collection),
		)
	}

	if c.spec.EndpointType == pf.EndpointType_INGEST {
		// Create a PushServer for the specification.
		// Careful! Don't assign directly to c.delegate because (*pc.PushServer)(nil) != nil
		var pushServer, err = pc.NewPushServer(
			c.taskTerm.ctx,
			newCombinerFn,
			c.labels.Range,
			&c.spec,
			c.labels.Build,
			startCommitFn,
		)
		if err != nil {
			return fmt.Errorf("opening push: %w", err)
		} else {
			c.delegate = pushServer
		}
	} else {
		// Establish driver connection and start Pull RPC.
		var err error
		c.driver, err = connector.NewDriver(
			c.taskTerm.ctx,
			c.spec.EndpointSpecJson,
			c.spec.EndpointType,
			c.opsPublisher,
			c.host.Config.Flow.Network,
		)
		if err != nil {
			return fmt.Errorf("building endpoint driver: %w", err)
		}

		// Open a Pull RPC stream for the capture.
		err = connector.WithUnsealed(c.driver, &c.spec, func(unsealed *pf.CaptureSpec) error {
			// Careful! Don't assign directly to c.delegate because (*pc.PullClient)(nil) != nil
			if pullClient, err := pc.OpenPull(
				c.taskTerm.ctx,
				c.driver.CaptureClient(),
				loadDriverCheckpoint(c.store),
				newCombinerFn,
				c.labels.Range,
				unsealed,
				c.labels.Build,
				!c.host.Config.Flow.Poll,
				startCommitFn,
			); err != nil {
				return err
			} else {
				c.delegate = pullClient
				return nil
			}
		})
		if err != nil {
			return fmt.Errorf("opening pull RPC: %w", err)
		}
	}

	ops.PublishLog(c.opsPublisher, pf.LogLevel_debug,
		"reading capture stream",
		"capture", c.labels.TaskName,
		"shard", c.shardSpec.Id,
		"build", c.labels.Build,
		"interval", minInterval,
	)
	return nil
}

// ReplayRange is not valid for a Capture and must not be called.
func (c *Capture) ReplayRange(_ consumer.Shard, _ pf.Journal, begin, end pf.Offset) message.Iterator {
	panic("ReplayRange is not valid for Capture runtime, and should never be called")
}

// ReadThrough returns its |offsets| unmodified.
func (c *Capture) ReadThrough(offsets pf.Offsets) (pf.Offsets, error) {
	return offsets, nil
}

func (c *Capture) ConsumeMessage(shard consumer.Shard, env message.Envelope, pub *message.Publisher) error {
	if env.Message.(*captureMessage).eof {
		// The connector exited; this is not a commit notification.
		c.delegateEOF = true // Mark for StartCommit.
		return nil
	}
	// This is a commit notification. The delegate has prepared combiners for each
	// binding with captured documents, and a DriverCheckpoint update.
	var combiners, driverCheckpoint = c.delegate.PopTransaction()

	if err := updateDriverCheckpoint(c.store, driverCheckpoint); err != nil {
		return err
	}

	// Walk each binding combiner, publishing captured documents and collecting stats.
	var mapper = flow.NewMapper(shard.Context(), c.host.Service.Etcd, c.host.Journals, shard.FQN())
	var bindingStats = make([]*pf.CombineAPI_Stats, 0, len(combiners))

	for b, combiner := range combiners {
		var binding = c.spec.Bindings[b]
		_ = binding.Collection // Elide nil check.

		var stats, err = combiner.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
			if full {
				panic("capture produces only partially combined documents")
			}

			partitions, err := tuple.Unpack(packedPartitions)
			if err != nil {
				return fmt.Errorf("unpacking partitions: %w", err)
			}

			_, err = pub.PublishUncommitted(mapper.Map, flow.Mappable{
				Spec:       &binding.Collection,
				Doc:        doc,
				PackedKey:  packedKey,
				Partitions: partitions,
			})
			if err != nil {
				return fmt.Errorf("publishing document: %w", err)
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("combiner.Drain: %w", err)
		}
		bindingStats = append(bindingStats, stats)
	}

	// Publish a final message with statistics about the capture transaction we'll soon finish, but
	// only if we've actually published any documents. It's possible for Capture transactions to
	// update the driver checkpoint without actually emitting any new documents. We choose not to
	// publish stats in that case in order to reduce noise in the stats collections, and simplify
	// working with them (readers of stats can safely assume that there will be capture stats when
	// kind = capture).
	var statsEvent = c.captureStats(bindingStats)
	if len(statsEvent.Capture) > 0 {
		var statsMessage = c.taskTerm.StatsFormatter.FormatEvent(statsEvent)
		if _, err := pub.PublishUncommitted(mapper.Map, statsMessage); err != nil {
			return fmt.Errorf("publishing stats document: %w", err)
		}
	} else {
		ops.PublishLog(c.opsPublisher, pf.LogLevel_debug,
			"capture transaction committing updating driver checkpoint only")
	}

	return nil
}

func (c *Capture) captureStats(statsPerBinding []*pf.CombineAPI_Stats) StatsEvent {
	var captureStats = make(map[string]CaptureBindingStats)
	for i, bindingStats := range statsPerBinding {
		// Skip bindings that didn't participate
		if bindingStats == nil {
			continue
		}
		var name = c.spec.Bindings[i].Collection.Collection.String()
		// It's possible for multiple bindings to use the same collection, in which case the
		// stats should be summed.
		var prevStats = captureStats[name]
		captureStats[name] = CaptureBindingStats{
			Right: prevStats.Right.with(bindingStats.Right),
			Out:   prevStats.Out.with(bindingStats.Out),
		}
	}
	// Prune stats for any collections that didn't have any data. This allows us to check
	// len(statsEvent.Capture) to see if the event contains any non-zero capture stats.
	for k, v := range captureStats {
		if v.Right.Docs == 0 {
			delete(captureStats, k)
		}
	}
	var event = c.NewStatsEvent()
	event.Capture = captureStats
	return event
}

// BeginTxn is a no-op.
func (c *Capture) BeginTxn(consumer.Shard) error {
	c.TxnOpened()
	return nil
}

// FinalizeTxn is a no-op.
func (c *Capture) FinalizeTxn(consumer.Shard, *message.Publisher) error { return nil }

// FinishedTxn logs if an error occurred.
func (c *Capture) FinishedTxn(_ consumer.Shard, op consumer.OpFuture) {
	logTxnFinished(c.opsPublisher, op)
}

// Coordinator panics if called.
func (c *Capture) Coordinator() *shuffle.Coordinator {
	panic("Coordinator is not valid for Capture runtime, and should never be called")
}

// StartCommit implements consumer.Store.StartCommit
func (c *Capture) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	ops.PublishLog(c.opsPublisher, pf.LogLevel_debug,
		"StartCommit",
		"capture", c.labels.TaskName,
		"shard", c.shardSpec.Id,
		"build", c.labels.Build,
	)

	var commitOp = c.store.StartCommit(shard, cp, waitFor)

	if c.delegateEOF {
		// This "transaction" was caused by an EOF from the delegate,
		// which was turned into a consumed message in order to update
		// the EOF pseudo-journal offset. The delegate's Serve loop
		// has already exited.
		c.delegateEOF = false // Reset.
	} else if err := c.delegate.SetLogCommitOp(commitOp); err != nil {
		// The delegate monitors |commitOp| to push acknowledgements to the
		// connector, and to unblock the commit of a current transaction.
		return client.FinishedOperation(err)
	}

	return commitOp
}

// Destroy implements consumer.Store.Destroy
func (c *Capture) Destroy() {
	if c.driver != nil {
		_ = c.driver.Close()
	}
	if c.delegate != nil {
		_ = c.delegate.Close()
	}
	c.taskTerm.destroy()
	c.store.Destroy()
}

// delegate is the common interface of PullClient and PushServer that we use.
type delegate interface {
	Close() error
	PopTransaction() ([]pf.Combiner, pf.DriverCheckpoint)
	SetLogCommitOp(op client.OpFuture) error
}

type captureMessage struct {
	clock message.Clock // Monotonic Clock counting capture transactions and exits.
	eof   bool          // True if the connector exited.
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
