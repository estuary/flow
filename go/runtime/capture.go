package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	pfc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pgc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Capture is a top-level Application which implements the capture workflow.
type Capture struct {
	// FlowConsumer which owns this Capture shard.
	host *FlowConsumer
	// Directory used for local processing files.
	localDir string
	// Store delegate for persisting local checkpoints.
	store *consumer.JSONFileStore
	// Embedded task processing state scoped to a current task revision.
	// Updated in RestoreCheckpoint.
	taskTerm
}

var _ Application = (*Capture)(nil)

// NewCaptureApp returns a new Capture, which implements Application.
func NewCaptureApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Capture, error) {
	var store, err = consumer.NewJSONFileStore(recorder, new(storeState))
	if err != nil {
		return nil, fmt.Errorf("consumer.NewJSONFileStore: %w", err)
	}

	return &Capture{
		host:     host,
		localDir: recorder.Dir(),
		store:    store,
		taskTerm: taskTerm{},
	}, nil
}

// RestoreCheckpoint initializes a catalog task term and restores the last
// persisted checkpoint, if any, by delegating to its JsonStore.
func (m *Capture) RestoreCheckpoint(shard consumer.Shard) (cp pgc.Checkpoint, err error) {
	if cp, err = m.store.RestoreCheckpoint(shard); err != nil {
		return pgc.Checkpoint{}, fmt.Errorf("store.RestoreCheckpoint: %w", err)
	}

	if m.taskTerm.revision == 0 {
		// This is our first task term of this shard assignment.
		// Captures don't have real journals. They synthesize pseudo-journals
		// which are used for consumer transaction plumbing, and to support use
		// with the Stat RPC (so we can Stat to block until a connector exits).
		// Reset these source checkpoints.
		cp.Sources = nil

		// A `nil` driver checkpoint will round-trip through JSON encoding as []byte("null").
		// Restore it's nil-ness after deserialization.
		if bytes.Equal([]byte("null"), m.store.State.(*storeState).DriverCheckpoint) {
			m.store.State.(*storeState).DriverCheckpoint = nil
		}
	}

	if err = m.taskTerm.initTerm(shard, m.host); err != nil {
		return cp, err
	} else if m.task.Capture == nil {
		return cp, fmt.Errorf("catalog task %q is not a capture", m.task.Name())
	}

	return cp, nil
}

// StartReadingMessages opens a captures stream with the specification's
// connector, and beings producing capture checkpoints into the
func (c *Capture) StartReadingMessages(shard consumer.Shard, cp pgc.Checkpoint,
	tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {

	// Build a context to capture under, and arrange for it to be cancelled
	// if the task definition is updated.
	var ctx, cancel = context.WithCancel(shard.Context())
	c.host.Catalog.SignalOnTaskUpdate(ctx,
		c.task.Name(), c.taskTerm.revision, cancel)

	var driverRx, err = c.openCapture(ctx)
	if err != nil {
		ch <- consumer.EnvelopeOrError{Error: err}
		return
	}

	var interval = time.Duration(c.task.Capture.IntervalSeconds) * time.Second
	log.WithFields(log.Fields{
		"shard":    shard.Spec().Id,
		"revision": c.taskTerm.revision,
		"interval": interval,
	}).Debug("opened capture stream")

	go c.serveDriverTransactions(ctx, shard.FQN(), time.NewTimer(interval).C, cp, driverRx, ch)
}

func (c *Capture) openCapture(ctx context.Context) (<-chan capture.CaptureResponse, error) {
	conn, err := capture.NewDriver(ctx,
		c.task.Capture.EndpointType,
		c.task.Capture.EndpointSpecJson,
		c.localDir,
	)
	if err != nil {
		return nil, fmt.Errorf("building endpoint driver: %w", err)
	}

	driverStream, err := conn.Capture(ctx, &pfc.CaptureRequest{
		Capture:              c.task.Capture,
		KeyBegin:             c.range_.KeyBegin,
		KeyEnd:               c.range_.KeyEnd,
		DriverCheckpointJson: c.store.State.(*storeState).DriverCheckpoint,
		Tail:                 !c.host.Config.Poll,
	})
	if err != nil {
		return nil, fmt.Errorf("driver.Capture: %w", err)
	}
	var driverRx = capture.CaptureResponseChannel(driverStream)

	var opened = <-driverRx
	if opened.Error != nil {
		return nil, fmt.Errorf("reading Opened: %w", opened.Error)
	} else if opened.Opened == nil {
		return nil, fmt.Errorf("expected Opened, got %#v", opened.CaptureResponse.String())
	}

	return driverRx, nil
}

func (c *Capture) serveDriverTransactions(
	ctx context.Context,
	fqn string,
	pollCh <-chan time.Time,
	cp pgc.Checkpoint,
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

	var txnJournal = &pb.JournalSpec{Name: pb.Journal(fmt.Sprintf("%s/txn", c.task.Capture.Capture))}
	var eofJournal = &pb.JournalSpec{Name: pb.Journal(fmt.Sprintf("%s/eof", c.task.Capture.Capture))}

	// Restore the largest Clock value previously recorded in the Checkpoint.
	var clock message.Clock
	for _, n := range []pb.Journal{txnJournal.Name, eofJournal.Name} {
		if src := cp.Sources[n]; src != nil {
			if c := message.Clock(src.ReadThrough); c > clock {
				clock = c
			}
		}
	}

	// Process transactions until the driver closes the stream,
	// or an error is encountered.
	for {
		var combiners, checkpoint, err = c.readTransaction(fqn, driverRx)
		clock.Tick()

		if err != nil {

			switch err {
			case io.EOF, context.Canceled:
				// No-op.
			default:
				// For now, we log these (only), and will retry the connector at its usual cadence.
				log.WithFields(log.Fields{
					"shard": fqn,
					"err":   err,
				}).Error("capture connector failed")
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
					clock:      clock,
					combiners:  combiners,
					checkpoint: checkpoint,
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
	// Checkpoint of this capture transaction.
	checkpoint json.RawMessage
}

func (m *captureMessage) GetUUID() message.UUID {
	return message.BuildUUID(message.ProducerID{}, m.clock, message.Flag_OUTSIDE_TXN)
}

func (m *captureMessage) SetUUID(message.UUID) {
	panic("must not be called")
}
func (m *captureMessage) NewAcknowledgement(pb.Journal) message.Message {
	panic("must not be called")
}

func (c *Capture) readTransaction(fqn string, ch <-chan capture.CaptureResponse,
) ([]*bindings.Combine, json.RawMessage, error) {

	// TODO(johnny): More efficient use of Combines:
	// * We ought to be re-using instances, which will matter more if Combines
	//   have longer-lived disk scratch files, etc.
	// * We could use consumer transaction back-pressure as a signal that there's
	//   opportunity to collapse multiple capture checkpoints into a single Combine,
	//   which may reduce produced data volumes.
	var combiners = make([]*bindings.Combine, len(c.task.Capture.Bindings))

	for i, b := range c.task.Capture.Bindings {
		combiners[i] = bindings.NewCombine()

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
			return combiners, resp.Commit.DriverCheckpointJson, nil
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

func (c *Capture) ReplayRange(_ consumer.Shard, _ pb.Journal, begin, end pb.Offset) message.Iterator {
	panic("ReplayRange is not valid for Capture runtime, and should never be called")
}

// ReadThrough returns its |offsets| unmodified.
func (c *Capture) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	return offsets, nil
}

func (c *Capture) ConsumeMessage(shard consumer.Shard, env message.Envelope, pub *message.Publisher) error {
	var mapper = flow.Mapper{
		Ctx:           shard.Context(),
		JournalClient: shard.JournalClient(),
		Journals:      c.host.Journals,
		JournalRules:  c.commons.JournalRules.Rules,
	}

	var msg = env.Message.(*captureMessage)

	if msg.eof {
		// The connector exited. This message is a no-op.
		return nil
	}

	for b, combiner := range msg.combiners {
		var binding = c.task.Capture.Bindings[b]
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
			return err
		})
		if err != nil {
			return err
		}
	}
	c.store.State.(*storeState).DriverCheckpoint = msg.checkpoint

	return nil
}

func (c *Capture) BeginTxn(consumer.Shard) error                        { return nil } // No-op.
func (c *Capture) FinalizeTxn(consumer.Shard, *message.Publisher) error { return nil } // No-op.
func (c *Capture) FinishedTxn(consumer.Shard, consumer.OpFuture)        {}             // No-op.

// Coordinator implements shuffle.Store.Coordinator
func (c *Capture) Coordinator() *shuffle.Coordinator {
	panic("Coordinator is not valid for Capture runtime, and should never be called")
}

// StartCommit implements consumer.Store.StartCommit
func (c *Capture) StartCommit(shard consumer.Shard, checkpoint pgc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	// Tell our JSON store to commit to its recovery log after |m.committed| resolves.
	return c.store.StartCommit(shard, checkpoint, waitFor)
}

// Destroy delegates to JSONStore.Destroy.
func (c *Capture) Destroy() {
	c.store.Destroy()
}
