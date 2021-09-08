package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/shuffle"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Materialize is a top-level Application which implements the materialization workflow.
type Materialize struct {
	// Combiners of the materialization, one for each current binding.
	combiners []*bindings.Combine
	// Operation started on each transaction StartCommit, which signals on
	// receipt of Committed from |driverRx|. It's used to sequence recovery log
	// commits, which it gates, while still allowing for optimistic pipelining.
	committed *client.AsyncOperation
	// Coordinator of shuffled reads for this materialization shard.
	coordinator *shuffle.Coordinator
	// FlowConsumer which owns this Materialize shard.
	host *FlowConsumer
	// Directory used for local processing files.
	localDir string
	// Driver responses, pumped through a concurrent read loop.
	// Updated in RestoreCheckpoint.
	driverRx <-chan materialize.TransactionResponse
	// Driver requests.
	// Updated in RestoreCheckpoint.
	driverTx pm.Driver_TransactionsClient
	// Flighted keys of the current transaction for each binding, plus a bounded number of
	// retained fully-reduced documents of the last transaction.
	// Updated in RestoreCheckpoint.
	flighted []map[string]json.RawMessage
	// Request is incrementally built and periodically sent by transaction
	// lifecycle functions.
	request *pm.TransactionRequest
	// Store delegate for persisting local checkpoints.
	store connectorStore
	// Embedded task processing state scoped to a current task revision.
	// Updated in RestoreCheckpoint.
	shuffleTaskTerm
}

var _ Application = (*Materialize)(nil)

// NewMaterializeApp returns a new Materialize, which implements Application.
func NewMaterializeApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Materialize, error) {
	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient(), host.Catalog)
	var store, err = newConnectorStore(recorder)
	if err != nil {
		return nil, fmt.Errorf("newConnectorStore: %w", err)
	}

	// Initialize into an already-committed state.
	var committed = client.NewAsyncOperation()
	committed.Resolve(nil)

	return &Materialize{
		combiners:       nil,
		committed:       committed,
		coordinator:     coordinator,
		host:            host,
		localDir:        recorder.Dir(),
		driverRx:        nil,
		driverTx:        nil,
		flighted:        nil,
		request:         nil,
		store:           store,
		shuffleTaskTerm: shuffleTaskTerm{},
	}, nil
}

// RestoreCheckpoint establishes a driver connection and begins a Transactions RPC.
// It queries the driver to select from the latest local or driver-persisted checkpoint.
func (m *Materialize) RestoreCheckpoint(shard consumer.Shard) (cp pc.Checkpoint, err error) {
	select {
	case <-m.committed.Done():
	default:
		// After a read drain, the Gazette consumer framework promises that a
		// prior commit fully completes before RestoreCheckpoint is called again.
		panic("prior commit is not done")
	}

	if m.driverTx != nil {
		_ = m.driverTx.CloseSend()
	}

	if err = m.initShuffleTerm(shard, m.host); err != nil {
		return cp, err
	} else if m.task.Materialization == nil {
		return cp, fmt.Errorf("catalog task %q is not a materialization", m.task.Name())
	}

	// Establish driver connection and start Transactions RPC.
	conn, err := materialize.NewDriver(shard.Context(),
		m.task.Materialization.EndpointType,
		m.task.Materialization.EndpointSpecJson,
		m.localDir,
		m.host.Config.ConnectorNetwork,
	)
	if err != nil {
		return pc.Checkpoint{}, fmt.Errorf("building endpoint driver: %w", err)
	}
	m.driverTx, err = conn.Transactions(shard.Context())
	if err != nil {
		return pc.Checkpoint{}, fmt.Errorf("driver.Transactions: %w", err)
	}
	m.driverRx = materialize.TransactionResponseChannel(m.driverTx)

	// Write Open request with locally persisted DriverCheckpoint.
	if err = pm.WriteOpen(
		m.driverTx,
		&m.request,
		m.task.Materialization,
		m.task.CommonsId,
		&m.range_,
		m.store.driverCheckpoint(),
	); err != nil {
		return pc.Checkpoint{}, err
	}

	// Read Opened response with driver's Checkpoint.
	var opened = <-m.driverRx
	if opened.Error != nil {
		return pc.Checkpoint{}, fmt.Errorf("reading Opened: %w", opened.Error)
	} else if opened.Opened == nil {
		return pc.Checkpoint{}, fmt.Errorf("expected Opened, got %#v",
			opened.TransactionResponse.String())
	}

	// Release left-over Combiners (if any), then initialize combiners and
	// "flighted" maps for each binding.
	for _, c := range m.combiners {
		c.Destroy()
	}
	m.combiners = m.combiners[:0]
	m.flighted = m.flighted[:0]

	for i, b := range m.task.Materialization.Bindings {
		m.combiners = append(m.combiners, bindings.NewCombine())
		m.flighted = append(m.flighted, make(map[string]json.RawMessage))

		if err = m.combiners[i].Configure(
			shard.FQN(),
			m.schemaIndex,
			b.Collection.Collection,
			b.Collection.SchemaUri,
			"", // Don't generate UUID placeholders.
			b.Collection.KeyPtrs,
			b.FieldValuePtrs(),
		); err != nil {
			return cp, fmt.Errorf("building combiner: %w", err)
		}
	}

	// If the store provided a Flow checkpoint, prefer that over
	// the |checkpoint| recovered from the local recovery log store.
	if b := opened.Opened.FlowCheckpoint; len(b) != 0 {
		if err = cp.Unmarshal(b); err != nil {
			return pc.Checkpoint{}, fmt.Errorf("unmarshal Opened.FlowCheckpoint: %w", err)
		}
	} else {
		// Otherwise restore locally persisted checkpoint.
		if cp, err = m.store.restoreCheckpoint(shard); err != nil {
			return pc.Checkpoint{}, fmt.Errorf("store.RestoreCheckpoint: %w", err)
		}
	}

	return cp, nil
}

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	// Write our intent to close the transaction and prepare for commit.
	// This signals the driver to send remaining Loaded responses, if any.
	if err := pm.WritePrepare(m.driverTx, &m.request, checkpoint); err != nil {
		return client.FinishedOperation(fmt.Errorf("sending Prepare: %w", err))
	}

	// Drain remaining Loaded responses, until we read Prepared.
	for {
		var next = <-m.driverRx
		if next.Error != nil {
			return client.FinishedOperation(fmt.Errorf(
				"reading Loaded or Prepared: %w", next.Error))
		} else if next.Loaded != nil {
			if err := m.reduceLoaded(next.Loaded); err != nil {
				return client.FinishedOperation(err)
			}
		} else if next.Prepared != nil {
			m.store.updateDriverCheckpoint(next.Prepared.DriverCheckpointMergePatchJson, true)
			break // All done.
		} else {
			// Protocol error.
			return client.FinishedOperation(fmt.Errorf(
				"expected Loaded or Prepared, got %#v",
				next.TransactionResponse.String(),
			))
		}
	}

	// Drain each binding.
	for i, b := range m.task.Materialization.Bindings {
		if err := drainBinding(
			m.flighted[i],
			m.combiners[i],
			b.DeltaUpdates,
			m.driverTx,
			&m.request,
			i,
		); err != nil {
			return client.FinishedOperation(err)
		}
	}

	// Wait for any |waitFor| operations. In practice this is always empty.
	// It would contain pending journal writes, but materializations don't issue any.
	for op := range waitFor {
		if op.Err() != nil {
			return client.FinishedOperation(fmt.Errorf("dependency failed: %w", op.Err()))
		}
	}

	if err := pm.WriteCommit(m.driverTx, &m.request); err != nil {
		return client.FinishedOperation(err)
	}

	// Spawn a task which awaits the Committed response (rather than blocking to wait).
	// This optimistically pipelines the next transaction while the store commits this one.
	m.committed = client.NewAsyncOperation()
	go awaitCommitted(m.driverRx, m.committed)

	// Tell our JSON store to commit to its recovery log after |m.committed| resolves.
	return m.store.startCommit(shard, checkpoint, consumer.OpFutures{m.committed: struct{}{}})
}

// drainBinding drains the a single materialization binding by sending Store
// requests for its reduced documents.
func drainBinding(
	flighted map[string]json.RawMessage,
	combiner *bindings.Combine,
	deltaUpdates bool,
	driverTx pm.Driver_TransactionsClient,
	request **pm.TransactionRequest,
	binding int,
) error {
	// Precondition: |flighted| contains the precise set of keys for this binding in this transaction.
	// See FinalizeTxn.
	var remaining = len(flighted)

	// Drain the combiner into materialization Store requests.
	if err := combiner.Drain(func(full bool, docRaw json.RawMessage, packedKey, packedValues []byte) error {
		// Inlined use of string(packedKey) clues compiler escape analysis to avoid allocation.
		if _, ok := flighted[string(packedKey)]; !ok {
			var key, _ = tuple.Unpack(packedKey)
			return fmt.Errorf(
				"driver implementation error: "+
					"loaded key %v was not requested by Flow in this transaction (document %s)",
				key,
				string(docRaw))
		}

		// We're using |full|, an indicator of whether the document was a full
		// reduction or a partial combine, to track whether the document exists
		// in the store. This works because we only issue reduce-left when a
		// document was provided by Loaded or was retained from a previous
		// transaction's Store.

		if err := pm.StageStore(driverTx, request, binding,
			packedKey, packedValues, docRaw, full,
		); err != nil {
			return err
		}

		// We can retain a bounded number of documents from this transaction
		// as a performance optimization, so that they may be directly available
		// to the next transaction without issuing a Load.
		if deltaUpdates || remaining >= cachedDocumentBound {
			delete(flighted, string(packedKey)) // Don't retain.
		} else {
			// We cannot reference |rawDoc| beyond this callback, and must copy.
			// Fortunately, StageStore did just that, appending the document
			// to the staged request Arena, which we can reference here because
			// Arena bytes are write-once.
			var s = (*request).Store
			flighted[string(packedKey)] = s.Arena.Bytes(s.DocsJson[len(s.DocsJson)-1])
		}

		remaining--
		return nil

	}); err != nil {
		return fmt.Errorf("combine.Finish: %w", err)
	}

	// We should have seen 1:1 combined documents for each flighted key.
	if remaining != 0 {
		log.WithFields(log.Fields{
			"remaining": remaining,
			"flighted":  len(flighted),
		}).Panic("combiner drained, but expected documents remainder != 0")
	}

	return nil
}

func awaitCommitted(driverRx <-chan materialize.TransactionResponse, result *client.AsyncOperation) {
	var m = <-driverRx

	if m.Error != nil {
		result.Resolve(fmt.Errorf("reading Committed: %w", m.Error))
	} else if m.Committed == nil {
		result.Resolve(fmt.Errorf("expected Committed, got %#v", m.TransactionResponse))
	} else {
		result.Resolve(nil)
	}
}

// Destroy implements consumer.Store.Destroy
func (m *Materialize) Destroy() {
	_ = m.driverTx.CloseSend()
	m.store.destroy()
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

// Coordinator implements shuffle.Store.Coordinator
func (m *Materialize) Coordinator() *shuffle.Coordinator {
	return m.coordinator
}

// Implementing runtime.Application for Materialize
var _ Application = (*Materialize)(nil)

// BeginTxn implements Application.BeginTxn and is a no-op.
func (m *Materialize) BeginTxn(shard consumer.Shard) error {
	return nil
}

// pollLoaded selects and processes Loaded responses which can be read without blocking.
func (m *Materialize) pollLoaded() error {
	for {
		var resp materialize.TransactionResponse
		select {
		case resp = <-m.driverRx:
		default:
			return nil
		}

		if resp.Error != nil {
			return fmt.Errorf("reading Loaded: %w", resp.Error)
		} else if resp.Loaded != nil {
			if err := m.reduceLoaded(resp.Loaded); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("expected Loaded, got %#v", resp.TransactionResponse)
		}
	}
}

// reduceLoaded reduces documents of the Loaded response into the matched combiner.
func (m *Materialize) reduceLoaded(loaded *pm.TransactionResponse_Loaded) error {
	var b = loaded.Binding
	if b >= uint32(len(m.task.Materialization.Bindings)) {
		return fmt.Errorf("driver error (binding %d out of range)", b)
	}
	var combiner = m.combiners[b]

	// Feed documents into the combiner as reduce-left operations.
	for _, slice := range loaded.DocsJson {
		if err := combiner.ReduceLeft(loaded.Arena.Bytes(slice)); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
	}
	return nil
}

// ConsumeMessage implements Application.ConsumeMessage
func (m *Materialize) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	select {
	case <-m.committed.Done():
		// Iff we've already read Committed from the last transaction,
		// do a non-blocking poll of ready Loaded responses.
		if err := m.pollLoaded(); err != nil {
			return fmt.Errorf("polling pending: %w", err)
		}
	default:
		// If a prior transaction hasn't committed, then an awaitCommitted() task
		// is still running and already selecting from |m.driverRx|.
	}

	var doc = envelope.Message.(pf.IndexedShuffleResponse)
	var packedKey = doc.Arena.Bytes(doc.PackedKey[doc.Index])
	var uuid = doc.GetUUID()

	if message.GetFlags(uuid) == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	// Find *Shuffle with equal pointer.
	var binding = -1 // Panic if no *Shuffle is matched.
	var flighted map[string]json.RawMessage
	var combiner *bindings.Combine
	var deltaUpdates bool

	for i, shuffle := range m.shuffles {
		if shuffle == doc.Shuffle {
			binding = i
			flighted = m.flighted[i]
			combiner = m.combiners[i]
			deltaUpdates = m.task.Materialization.Bindings[i].DeltaUpdates
		}
	}

	if doc, ok := flighted[string(packedKey)]; ok && doc == nil {
		// We've already seen this key within this transaction.
	} else if ok {
		// We retained this document from the last transaction.
		if deltaUpdates {
			panic("we shouldn't have retained if deltaUpdates")
		}
		if err := combiner.ReduceLeft(doc); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
		flighted[string(packedKey)] = nil // Clear old value & mark as visited.
	} else {
		// This is a novel key.
		if !deltaUpdates {
			if err := pm.StageLoad(m.driverTx, &m.request, binding, packedKey); err != nil {
				return err
			}
		}
		flighted[string(packedKey)] = nil // Mark as visited.
	}

	if err := combiner.CombineRight(doc.Arena.Bytes(doc.DocsJson[doc.Index])); err != nil {
		return fmt.Errorf("combiner.CombineRight: %w", err)
	}

	return nil
}

// FinalizeTxn implements Application.FinalizeTxn
func (m *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	// Transactions may begin only after the last has committed.
	select {
	case <-m.committed.Done(): // Pass.
	default:
		panic("committed is not Done")
	}

	// Any remaining flighted keys *not* having `nil` values are retained documents
	// of a prior transaction which were not updated during this one.
	// We garbage collect them here, and achieve the StartCommit precondition that
	// |m.flighted| holds only keys of the current transaction with `nil` sentinels.
	for _, flighted := range m.flighted {
		for key, doc := range flighted {
			if doc != nil {
				delete(flighted, key)
			}
		}
	}

	log.WithFields(log.Fields{
		"shard":    shard.Spec().Id,
		"flighted": len(m.flighted),
	}).Trace("FinalizeTxn")

	return nil
}

// FinishedTxn implements Application.FinishedTxn
func (m *Materialize) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {}

// TODO(johnny): This is an interesting knob that should be exposed.
const cachedDocumentBound = 2048
