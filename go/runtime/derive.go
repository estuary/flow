package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/shuffle"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Derive is a top-level Application which implements the derivation workflow.
type Derive struct {
	// Derive binding that's used for the life of the derivation shard.
	binding *bindings.Derive
	// Coordinator of shuffled reads for this derivation shard.
	coordinator *shuffle.Coordinator
	// FlowConsumer which owns this Derive shard.
	host *FlowConsumer
	// Instrumented RocksDB recorder.
	recorder *recoverylog.Recorder
	// Embedded task processing state scoped to a current task revision.
	// Updated in RestoreCheckpoint.
	shuffleTaskTerm
}

var _ Application = (*Derive)(nil)

// NewDeriveApp builds and returns a *Derive Application.
func NewDeriveApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Derive, error) {
	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient(), host.Catalog)

	var binding, err = bindings.NewDerive(recorder, recorder.Dir())
	if err != nil {
		return nil, err
	}

	var derive = &Derive{
		binding:         binding,
		coordinator:     coordinator,
		host:            host,
		recorder:        recorder,
		shuffleTaskTerm: shuffleTaskTerm{},
	}
	return derive, nil
}

// RestoreCheckpoint initializes a processing term for the derivation,
// configures the API binding delegate, and restores the last checkpoint.
// It implements the consumer.Store interface.
func (d *Derive) RestoreCheckpoint(shard consumer.Shard) (cp pc.Checkpoint, err error) {
	if err = d.initShuffleTerm(shard, d.host); err != nil {
		return cp, err
	} else if d.task.Derivation == nil {
		return cp, fmt.Errorf("catalog task %q is not a derivation", d.task.Name())
	}

	typeScriptClient, err := d.commons.TypeScriptClient(d.host.Service.Etcd)
	if err != nil {
		return cp, fmt.Errorf("building TypeScript client: %w", err)
	}
	err = d.binding.Configure(shard.FQN(), d.schemaIndex, d.task.Derivation, typeScriptClient)
	if err != nil {
		return cp, fmt.Errorf("configuring derive API: %w", err)
	}

	return d.binding.RestoreCheckpoint()
}

// Destroy releases the API binding delegate, which also cleans up the associated
// Rust-held RocksDB and its files.
func (d *Derive) Destroy() {
	d.binding.Destroy()
}

// BeginTxn begins a derive transaction.
func (d *Derive) BeginTxn(shard consumer.Shard) error {
	d.binding.BeginTxn()
	return nil
}

// ConsumeMessage passes the message to the derive worker.
func (d *Derive) ConsumeMessage(_ consumer.Shard, env message.Envelope, _ *message.Publisher) error {
	var doc = env.Message.(pf.IndexedShuffleResponse)
	var uuid = doc.UuidParts[doc.Index]

	for index, shuffle := range d.shuffles {
		// Find *Shuffle with equal pointer.
		if shuffle == doc.Shuffle {
			return d.binding.Add(
				uuid,
				doc.Arena.Bytes(doc.PackedKey[doc.Index]),
				uint32(index),
				doc.Arena.Bytes(doc.DocsJson[doc.Index]),
			)
		}
	}
	panic("matching shuffle not found")
}

// FinalizeTxn finishes and drains the derive worker transaction,
// and publishes each combined document to the derived collection.
func (d *Derive) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	var mapper = flow.Mapper{
		Ctx:           shard.Context(),
		JournalClient: shard.JournalClient(),
		Journals:      d.host.Journals,
		JournalRules:  d.commons.JournalRules.Rules,
	}
	var collection = &d.task.Derivation.Collection

	return d.binding.Drain(func(full bool, doc json.RawMessage, packedKey, packedPartitions []byte) error {
		if full {
			panic("derivation produces only partially combined documents")
		}

		partitions, err := tuple.Unpack(packedPartitions)
		if err != nil {
			return fmt.Errorf("unpacking partitions: %w", err)
		}
		_, err = pub.PublishUncommitted(mapper.Map, flow.Mappable{
			Spec:       collection,
			Doc:        doc,
			PackedKey:  packedKey,
			Partitions: partitions,
		})
		return err
	})
}

// StartCommit implements the Store interface, and writes the current transaction
// as an atomic RocksDB WriteBatch, guarded by a write barrier.
func (d *Derive) StartCommit(_ consumer.Shard, cp pc.Checkpoint, waitFor client.OpFutures) client.OpFuture {
	// Install a barrier such that we don't begin writing until |waitFor| has resolved.
	_ = d.recorder.Barrier(waitFor)

	// Ask the worker to apply its rocks WriteBatch, with our marshalled Checkpoint.
	if err := d.binding.PrepareCommit(cp); err != nil {
		return client.FinishedOperation(err)
	}
	// Another barrier which notifies when the WriteBatch
	// has been durably recorded to the recovery log.
	return d.recorder.Barrier(nil)
}

// FinishedTxn is a no-op.
func (d *Derive) FinishedTxn(_ consumer.Shard, _ consumer.OpFuture) {}

// Coordinator returns the shard's *shuffle.Coordinator.
func (d *Derive) Coordinator() *shuffle.Coordinator { return d.coordinator }

// ClearRegistersForTest delegates the request to its worker.
func (d *Derive) ClearRegistersForTest() error {
	return d.binding.ClearRegisters()
}
