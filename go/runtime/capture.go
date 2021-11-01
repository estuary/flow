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
	// FlowConsumer which owns this Capture shard.
	host *FlowConsumer
	// Store delegate for persisting local checkpoints.
	store connectorStore
	// Active capture specification, updated in RestoreCheckpoint.
	capture *pf.CaptureSpec
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
		host:     host,
		store:    store,
		taskTerm: taskTerm{},
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

	if err = c.build.Extract(func(db *sql.DB) error {
		c.capture, err = catalog.LoadCapture(db, c.labels.TaskName)
		return err
	}); err != nil {
		return pf.Checkpoint{}, err
	}

	if cp, err = c.store.restoreCheckpoint(shard); err != nil {
		return pf.Checkpoint{}, err
	}

	// Captures don't have real journals. They synthesize pseudo-journals
	// which are used for consumer transaction plumbing, and to support use
	// with the Stat RPC (so we can Stat to block until a connector exits).
	// Reset these source checkpoints.
	cp.Sources = nil

	return cp, nil
}

// StartReadingMessages opens a captures stream with the specification's
// connector, and beings producing capture checkpoints into the
func (c *Capture) StartReadingMessages(shard consumer.Shard, cp pf.Checkpoint,
	tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {

	// Build a context to capture under, and arrange for it to be cancelled
	// if the shard specification is updated.
	var ctx, cancel = context.WithCancel(shard.Context())
	signalOnSpecUpdate(c.host.Service.State.KS, shard, c.shardSpec, cancel)

	var driverRx, err = c.openCapture(ctx)
	if err != nil {
		c.Log(log.ErrorLevel, log.Fields{
			"error": err.Error(),
		}, "failed to open capture")
		ch <- consumer.EnvelopeOrError{Error: err}
		return
	}

	var interval = time.Duration(c.capture.IntervalSeconds) * time.Second
	c.Log(log.DebugLevel, log.Fields{
		"capture":  c.labels.TaskName,
		"shard":    c.shardSpec.Id,
		"build":    c.labels.Build,
		"interval": interval,
	}, "opened capture stream")
	go c.serveDriverTransactions(ctx, shard.FQN(), time.NewTimer(interval).C, cp, driverRx, ch)
}

func (c *Capture) openCapture(ctx context.Context) (<-chan capture.CaptureResponse, error) {
	conn, err := capture.NewDriver(ctx,
		c.capture.EndpointType,
		c.capture.EndpointSpecJson,
		c.host.Config.Flow.Network,
		c.LogPublisher,
	)
	if err != nil {
		return nil, fmt.Errorf("building endpoint driver: %w", err)
	}

	driverStream, err := conn.Capture(ctx, &pc.CaptureRequest{
		Capture:              c.capture,
		KeyBegin:             c.labels.Range.KeyBegin,
		KeyEnd:               c.labels.Range.KeyEnd,
		DriverCheckpointJson: c.store.driverCheckpoint(),
		Tail:                 !c.host.Config.Flow.Poll,
	})
	if err != nil {
		return nil, fmt.Errorf("driver.Capture: %w", err)
	}
	var driverRx = capture.CaptureResponseChannel(driverStream)

	if opened, err := capture.Rx(driverRx, true); err != nil {
		return nil, fmt.Errorf("reading Opened: %w", err)
	} else if opened.Opened == nil {
		return nil, fmt.Errorf("expected Opened, got %#v", opened.String())
	}

	return driverRx, nil
}

func (c *Capture) serveDriverTransactions(
	ctx context.Context,
	fqn string,
	pollCh <-chan time.Time,
	cp pf.Checkpoint,
	driverRx <-chan capture.CaptureResponse,
	envelopeTx chan<- consumer.EnvelopeOrError,
) {
	defer close(envelopeTx)

	// A consumer.Envelope requires a JournalSpec, of which only the Name is actually
	// used (for sequencing messages and producing checkpoints).
	// Of course, captures don't actually have a journal from which they read,
	// so invent minimal JournalSpecs which interoperate with the `consumer`
	// package. These pseudo-specs model connector transactions and exits.
	//
	// In the future, we *may* want to generalize the `consumer` package to decouple
	// its current tight binding with JournalSpecs.

	var txnJournal = &pf.JournalSpec{Name: pf.Journal(fmt.Sprintf("%s/txn", c.capture.Capture))}
	var eofJournal = &pf.JournalSpec{Name: pf.Journal(fmt.Sprintf("%s/eof", c.capture.Capture))}

	// Restore the largest Clock value previously recorded in the Checkpoint.
	var clock message.Clock
	for _, n := range []pf.Journal{txnJournal.Name, eofJournal.Name} {
		if c := message.Clock(cp.Sources[n].ReadThrough); c > clock {
			clock = c
		}
	}

	// Process transactions until the driver closes the stream,
	// or an error is encountered.
	for {
		var combiners, commit, err = c.readTransaction(fqn, driverRx)
		clock.Tick()

		if err != nil {

			switch err {
			case io.EOF, context.Canceled:
				// No-op.
			default:
				// For now, we log these (only), and will retry the connector at its usual cadence.
				c.Log(log.ErrorLevel, log.Fields{
					"error": err.Error(),
				}, "capture connector failed (will retry)")
			}

			// Emit a no-op message. Its purpose is only to update the tracked EOF offset,
			// which may unblock an associated shard Stat RPC.
			envelopeTx <- consumer.EnvelopeOrError{
				Envelope: message.Envelope{
					Journal: eofJournal,
					Begin:   int64(clock),
					End:     int64(clock + 1),
					Message: &captureMessage{
						clock: clock,
						eof:   true,
					},
				},
			}

			// We have a deferred close of |envelopeTx|, and returning will drain
			// the current task term and start another. That shouldn't happen until
			// the configured polling interval is elapsed (or the context is cancelled).
			select {
			case <-pollCh:
			case <-ctx.Done():
			}

			return
		}

		envelopeTx <- consumer.EnvelopeOrError{
			Envelope: message.Envelope{
				Journal: txnJournal,
				Begin:   int64(clock),
				End:     int64(clock + 1),
				Message: &captureMessage{
					clock:     clock,
					combiners: combiners,
					commit:    commit,
				},
			},
		}
	}
}

type captureMessage struct {
	// Monotonic Clock counting capture transactions and exits.
	clock message.Clock
	// True if the connector exited gracefully, in which case combiners and checkpoint are nil.
	eof bool
	// Combined documents of this capture transaction.
	combiners []*bindings.Combine
	// Commit of this capture transaction.
	commit *pc.CaptureResponse_Commit
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

func (c *Capture) readTransaction(fqn string, ch <-chan capture.CaptureResponse,
) (_ []*bindings.Combine, _ *pc.CaptureResponse_Commit, err error) {

	// TODO(johnny): More efficient use of Combines:
	// * We ought to be re-using instances, which will matter more if Combines
	//   have longer-lived disk scratch files, etc.
	// * We could use consumer transaction back-pressure as a signal that there's
	//   opportunity to collapse multiple capture checkpoints into a single Combine,
	//   which may reduce produced data volumes.
	var combiners = make([]*bindings.Combine, len(c.capture.Bindings))

	// Ensure that partial combiners are destroyed if an error is returned.
	defer func() {
		if err == nil {
			return
		}
		for _, c := range combiners {
			if c != nil {
				c.Destroy()
			}
		}
	}()

	for i, b := range c.capture.Bindings {
		combiners[i], err = bindings.NewCombine(c.LogPublisher)
		if err != nil {
			return nil, nil, fmt.Errorf("creating combiner: %w", err)
		}

		if err := combiners[i].Configure(
			fqn,
			c.schemaIndex,
			b.Collection.Collection,
			b.Collection.SchemaUri,
			b.Collection.UuidPtr,
			b.Collection.KeyPtrs,
			flow.PartitionPointers(&b.Collection),
		); err != nil {
			return nil, nil, fmt.Errorf("configuring combiner: %w", err)
		}
	}

	for resp := range ch {
		if resp.Error != nil {
			return nil, nil, resp.Error
		} else if resp.Commit != nil {
			return combiners, resp.Commit, nil
		} else if resp.Captured == nil {
			return nil, nil, fmt.Errorf("expected Captured or Commit, got %#v", resp.String())
		}

		var b = int(resp.Captured.Binding)
		if b >= len(combiners) {
			return nil, nil, fmt.Errorf("driver error (binding %d out of range)", b)
		}
		var combiner = combiners[b]

		// Feed documents into the combiner as combine-right operations.
		for _, slice := range resp.Captured.DocsJson {
			if err := combiner.CombineRight(resp.Captured.Arena.Bytes(slice)); err != nil {
				return nil, nil, fmt.Errorf("combiner.CombineRight: %w", err)
			}
		}
	}
	return nil, nil, io.EOF
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
	var mapper = flow.NewMapper(shard.Context(), shard.JournalClient(), c.host.Journals)
	var msg = env.Message.(*captureMessage)

	if msg.eof {
		// The connector exited. This message is a no-op.
		return nil
	}

	for b, combiner := range msg.combiners {
		var binding = c.capture.Bindings[b]
		_ = binding.Collection // Elide nil check.
		defer combiner.Destroy()

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
	c.store.updateDriverCheckpoint(
		msg.commit.DriverCheckpointJson,
		msg.commit.Rfc7396MergePatch)

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

// Coordinator implements shuffle.Store.Coordinator
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
	return c.store.startCommit(shard, cp, waitFor)
}

// Destroy implements consumer.Store.Destroy
func (c *Capture) Destroy() {
	c.taskTerm.destroy()
	c.store.destroy()
}
