package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/shuffle"
	pc "github.com/estuary/protocols/capture"
	"github.com/estuary/protocols/catalog"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Capture is a top-level Application which implements the capture workflow.
type Capture struct {
	// Client of the driver Pull RPC.
	client *pc.PullClient
	// FlowConsumer which owns this Capture shard.
	host *FlowConsumer
	// Store delegate for persisting local checkpoints.
	store connectorStore
	// Specification under which the capture is currently running.
	spec pf.CaptureSpec
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
		client:   nil, // Initialized in RestoreCheckpoint.
		host:     host,
		store:    store,
		spec:     pf.CaptureSpec{}, // Initialized in RestoreCheckpoint.
		taskTerm: taskTerm{},       // Initialized in RestoreCheckpoint.
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
			c.Log(log.DebugLevel, log.Fields{
				"capture":    c.labels.TaskName,
				"shard":      c.shardSpec.Id,
				"build":      c.labels.Build,
				"checkpoint": cp,
			}, "initialized processing term")

		} else {
			c.Log(log.ErrorLevel, log.Fields{
				"error": err.Error(),
			}, "failed to initialize processing term")
		}
	}()

	if c.client == nil {
		// First initialization.
	} else if err := c.client.Close(); err != nil {
		return pf.Checkpoint{}, fmt.Errorf("stopping previous client: %w", err)
	}

	if err = c.build.Extract(func(db *sql.DB) error {
		if s, err := catalog.LoadCapture(db, c.labels.TaskName); err != nil {
			return err
		} else {
			c.spec = *s
			return nil
		}
	}); err != nil {
		return pf.Checkpoint{}, err
	}

	// Establish driver connection and start Pull RPC.
	conn, err := capture.NewDriver(
		shard.Context(),
		c.spec.EndpointType,
		c.spec.EndpointSpecJson,
		c.host.Config.Flow.Network,
		c.LogPublisher,
	)
	if err != nil {
		return pf.Checkpoint{}, fmt.Errorf("building endpoint driver: %w", err)
	}

	// Closure which builds a Combiner for a specified binding.
	var newCombinerFn = func(binding *pf.CaptureSpec_Binding) (pf.Combiner, error) {
		var combiner, err = bindings.NewCombine(c.LogPublisher)
		if err != nil {
			return nil, err
		}
		return combiner, combiner.Configure(
			shard.FQN(),
			c.schemaIndex,
			binding.Collection.Collection,
			binding.Collection.SchemaUri,
			binding.Collection.UuidPtr,
			binding.Collection.KeyPtrs,
			flow.PartitionPointers(&binding.Collection),
		)
	}

	// Build a context to capture under, and arrange for it to be cancelled
	// if the shard specification is updated.
	var ctx, cancel = context.WithCancel(shard.Context())
	go signalOnSpecUpdate(c.host.Service.State.KS, shard, c.shardSpec, cancel)

	// Open a Pull RPC stream for the capture under this context.
	c.client, err = pc.OpenPull(
		ctx,
		conn,
		c.store.driverCheckpoint(),
		newCombinerFn,
		c.labels.Range,
		&c.spec,
		c.labels.Build,
		!c.host.Config.Flow.Poll,
	)
	if err != nil {
		return pf.Checkpoint{}, fmt.Errorf("opening pull RPC: %w", err)
	}

	if cp, err = c.store.restoreCheckpoint(shard); err != nil {
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

		switch err {
		case io.EOF:
			// This is a graceful close of the capture. Emit a no-op message,
			// whose purpose is only to update the tracked EOF offset,
			// which may in turn unblock an associated shard Stat RPC.
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

		case context.Canceled:
			// Don't log.

		default:
			// Remaining errors are logged but not otherwise acted upon.
			// We'll retry the connector at its next configured poll interval.
			c.Log(log.ErrorLevel, log.Fields{"error": err.Error()},
				"capture connector failed (will retry)")
		}

		// Close |ch| to signal completion of the stream, which will drain the
		// current task term and start another. But, that shouldn't happen until
		// the configured minimum polling interval elapses.
		select {
		case <-minTimer.C:
		case <-shard.Context().Done():
			ch <- consumer.EnvelopeOrError{Error: shard.Context().Err()}
		}

		close(ch)
		return
	}

	go c.client.Read(startCommitFn)

	c.Log(log.DebugLevel, log.Fields{
		"capture":  c.labels.TaskName,
		"shard":    c.shardSpec.Id,
		"build":    c.labels.Build,
		"interval": minInterval,
	}, "reading capture stream")
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
		return nil // The connector exited; this is not a commit notification.
	}

	var mapper = flow.NewMapper(shard.Context(), c.host.Service.Etcd, c.host.Journals, shard.FQN())

	for b, combiner := range c.client.Combiners() {
		var binding = c.spec.Bindings[b]
		_ = binding.Collection // Elide nil check.

		var err = combiner.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
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
	}

	return nil
}

// BeginTxn is a no-op.
func (c *Capture) BeginTxn(consumer.Shard) error { return nil }

// FinalizeTxn is a no-op.
func (c *Capture) FinalizeTxn(consumer.Shard, *message.Publisher) error { return nil }

// FinishedTxn logs if an error occurred.
func (c *Capture) FinishedTxn(_ consumer.Shard, op consumer.OpFuture) {
	logTxnFinished(c.LogPublisher, op)
}

// Coordinator panics if called.
func (c *Capture) Coordinator() *shuffle.Coordinator {
	panic("Coordinator is not valid for Capture runtime, and should never be called")
}

// StartCommit implements consumer.Store.StartCommit
func (c *Capture) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	c.Log(log.DebugLevel, log.Fields{
		"capture":    c.labels.TaskName,
		"shard":      c.shardSpec.Id,
		"build":      c.labels.Build,
		"checkpoint": cp,
	}, "StartCommit")

	var commitOp = c.store.startCommit(shard, cp, c.client.DriverCheckpoint(), waitFor)

	// The client monitors |commitOp| to push acknowledgements to the connector,
	// and to unblock the commit of a current transaction. It's expected that
	// SetLogCommitOp will return EOF on a graceful server-initiated close of the
	// RPC. We ignore other errors as well because they're reported to our
	// startCommitFn callback.
	_ = c.client.SetLogCommitOp(commitOp)

	return commitOp
}

// Destroy implements consumer.Store.Destroy
func (c *Capture) Destroy() {
	if c.client != nil {
		_ = c.client.Close()
	}
	c.taskTerm.destroy()
	c.store.destroy()
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
