package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	store_rocksdb "go.gazette.dev/core/consumer/store-rocksdb"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// Derive wires the high-level runtime of the derive consumer flow.
type Derive struct {
	readBuilder *shuffle.ReadBuilder
	mapper      flow.Mapper
	derivation  pf.CollectionSpec
	coordinator *shuffle.Coordinator
	worker      *bindings.Derive
	recorder    *recoverylog.Recorder
}

var _ Application = (*Derive)(nil)

// NewDeriveApp builds and returns a *Derive Application.
func NewDeriveApp(
	service *consumer.Service,
	journals *keyspace.KeySpace,
	shard consumer.Shard,
	recorder *recoverylog.Recorder,
) (*Derive, error) {
	var catalogURL, err = shardLabel(shard, labels.CatalogURL)
	if err != nil {
		return nil, err
	}
	catalog, err := flow.NewCatalog(catalogURL, recorder.Dir)
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}
	defer catalog.Close()

	derivation, err := shardLabel(shard, labels.Derivation)
	if err != nil {
		return nil, err
	}
	spec, err := catalog.LoadDerivedCollection(derivation)
	if err != nil {
		return nil, fmt.Errorf("loading collection spec: %w", err)
	}
	transforms, err := catalog.LoadTransforms(derivation)
	if err != nil {
		return nil, fmt.Errorf("loading transform specs: %w", err)
	}
	readBuilder, err := shuffle.NewReadBuilder(service, journals, shard,
		shuffle.ReadSpecsFromTransforms(transforms))
	if err != nil {
		return nil, fmt.Errorf("NewReadBuilder: %w", err)
	}

	var mapper = flow.Mapper{
		Ctx:           shard.Context(),
		JournalClient: shard.JournalClient(),
		Journals:      journals,
	}

	worker, err := bindings.NewDerive(
		catalog.LocalPath(),
		spec.Name.String(),
		recorder.Dir,
		spec.UuidPtr,
		flow.PartitionPointers(&spec),
		store_rocksdb.NewHookedEnv(store_rocksdb.NewRecorder(recorder)),
	)
	if err != nil {
		return nil, fmt.Errorf("building derive worker: %w", err)
	}

	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient())

	return &Derive{
		readBuilder: readBuilder,
		mapper:      mapper,
		derivation:  spec,
		coordinator: coordinator,
		worker:      worker,
		recorder:    recorder,
	}, nil
}

// RestoreCheckpoint implements the Store interface, delegating to the worker.
func (a *Derive) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	return a.worker.RestoreCheckpoint()
}

// Destroy implements the Store interface. It gracefully stops the flow-worker.
func (a *Derive) Destroy() {
	a.worker.Stop()
}

// BeginTxn begins a derive transaction.
func (a *Derive) BeginTxn(shard consumer.Shard) error {
	a.worker.BeginTxn()
	return nil
}

// ConsumeMessage passes the message to the derive worker.
func (a *Derive) ConsumeMessage(_ consumer.Shard, env message.Envelope, _ *message.Publisher) error {
	var doc = env.Message.(pf.IndexedShuffleResponse)
	var uuid = doc.UuidParts[doc.Index]

	if err := a.worker.Add(
		uuid,
		doc.Arena.Bytes(doc.PackedKey[doc.Index]),
		doc.Transform.ReaderCatalogDBID,
		doc.Arena.Bytes(doc.DocsJson[doc.Index]),
	); err != nil {
		return err
	}

	if message.Flags(uuid.ProducerAndFlags)&message.Flag_ACK_TXN != 0 {
		return a.worker.Flush()
	}
	return nil
}

// FinalizeTxn finishes and drains the derive worker transaction,
// and publishes each combined document to the derived collection.
func (a *Derive) FinalizeTxn(_ consumer.Shard, pub *message.Publisher) error {
	return a.worker.Finish(func(doc json.RawMessage, packedKey []byte, partitions tuple.Tuple) error {
		var _, err = pub.PublishUncommitted(a.mapper.Map, flow.Mappable{
			Spec:       &a.derivation,
			Doc:        doc,
			PackedKey:  packedKey,
			Partitions: partitions,
		})
		return err
	})
}

// StartCommit implements the Store interface, and writes the current transaction
// as an atomic RocksDB WriteBatch, guarded by a write barrier.
func (a *Derive) StartCommit(_ consumer.Shard, cp pc.Checkpoint, waitFor client.OpFutures) client.OpFuture {
	// Install a barrier such that we don't begin writing until |waitFor| has resolved.
	_ = a.recorder.Barrier(waitFor)

	// Ask the worker to apply its rocks WriteBatch, with our marshalled Checkpoint.
	if err := a.worker.PrepareCommit(cp); err != nil {
		return client.FinishedOperation(err)
	}
	// Another barrier which notifies when the WriteBatch
	// has been durably recorded to the recovery log.
	return a.recorder.Barrier(nil)
}

// FinishedTxn resets the current derive RPC.
func (a *Derive) FinishedTxn(_ consumer.Shard, _ consumer.OpFuture) {
	// No-op.
}

// StartReadingMessages delegates to shuffle.StartReadingMessages.
func (a *Derive) StartReadingMessages(shard consumer.Shard, cp pc.Checkpoint,
	tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {

	shuffle.StartReadingMessages(shard.Context(), a.readBuilder, cp, tp, ch)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (a *Derive) ReplayRange(shard consumer.Shard, journal pb.Journal,
	begin pb.Offset, end pb.Offset) message.Iterator {

	return a.readBuilder.StartReplayRead(shard.Context(), journal, begin, end)
}

// ReadThrough delegates to shuffle.ReadThrough.
func (a *Derive) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	return a.readBuilder.ReadThrough(offsets)
}

// Coordinator returns the App's shared *shuffle.Coordinator.
func (a *Derive) Coordinator() *shuffle.Coordinator { return a.coordinator }

// ClearRegisters delegates the request to its worker.
func (a *Derive) ClearRegisters(ctx context.Context,
	req *pf.ClearRegistersRequest) (*pf.ClearRegistersResponse, error) {

	var err = a.worker.ClearRegisters()
	return new(pf.ClearRegistersResponse), err
}

func shardLabel(shard consumer.Shard, label string) (string, error) {
	var values = shard.Spec().LabelSet.ValuesOf(label)
	if len(values) != 1 {
		return "", fmt.Errorf("expected single shard label %q (got %s)", label, values)
	}
	return values[0], nil
}
