package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize/driver"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// Materialize is the high-level runtime of the materialization consumer workflow.
type Materialize struct {
	// Transaction-scoped combiner.
	combiner        *bindings.Combine
	combinerBuilder *bindings.CombineBuilder
	// Operation started on each transaction StartCommit, which signals on
	// receipt of Committed from |driverRx|. It's used to sequence recovery log
	// commits, which it gates, while still allowing for optimistic pipelining.
	committed   *client.AsyncOperation
	coordinator *shuffle.Coordinator
	// If |deltaUpdates|, we materialize combined delta-update documents of
	// keys, and not full reductions. We don't issue loads, and don't retain
	// a cache of documents across transactions. Set in RestoreCheckpoint.
	deltaUpdates bool
	// Driver responses, pumped through a concurrent read loop.
	driverRx <-chan driver.TransactionResponse
	// Driver requests.
	driverTx pm.Driver_TransactionsClient
	// Flighted keys of the current transaction, plus a bounded number of
	// retained fully-reduced documents of the last transaction.
	flighted    map[string]json.RawMessage
	readBuilder *shuffle.ReadBuilder
	recorder    *recoverylog.Recorder
	// Request is incrementally built and periodically sent by transaction
	// lifecycle functions.
	request *pm.TransactionRequest
	spec    *pf.MaterializationSpec
	store   *consumer.JSONFileStore
}

type storeState struct {
	DriverCheckpoint []byte
}

// NewMaterializeApp returns a new Materialize, which implements Application
func NewMaterializeApp(
	service *consumer.Service,
	journals *keyspace.KeySpace,
	shard consumer.Shard,
	recorder *recoverylog.Recorder,
) (*Materialize, error) {
	var catalogURL, err = shardLabel(shard, labels.CatalogURL)
	if err != nil {
		return nil, err
	}
	materializationName, err := shardLabel(shard, labels.Materialization)
	if err != nil {
		return nil, err
	}

	// Open catalog and load required specs.
	catalog, err := flow.NewCatalog(catalogURL, recorder.Dir())
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}
	defer catalog.Close()

	spec, err := catalog.LoadMaterialization(materializationName)
	if err != nil {
		return nil, fmt.Errorf("loading materialization spec: %w", err)
	}
	schemaBundle, err := catalog.LoadSchemaBundle()
	if err != nil {
		return nil, fmt.Errorf("loading schema bundle: %w", err)
	}
	schemaIndex, err := bindings.NewSchemaIndex(schemaBundle)
	if err != nil {
		return nil, fmt.Errorf("building schema index: %w", err)
	}
	readBuilder, err := shuffle.NewReadBuilder(service, journals, shard,
		[]*pf.Shuffle{&spec.Shuffle})
	if err != nil {
		return nil, fmt.Errorf("NewReadBuilder: %w", err)
	}

	store, err := consumer.NewJSONFileStore(recorder, new(storeState))
	if err != nil {
		return nil, fmt.Errorf("consumer.NewJSONFileStore: %w", err)
	}
	conn, err := driver.NewDriver(shard.Context(), spec.EndpointType, json.RawMessage(spec.EndpointConfig))
	if err != nil {
		return nil, fmt.Errorf("building endpoint driver: %w", err)
	}
	driverTx, err := conn.Transactions(shard.Context())
	if err != nil {
		return nil, fmt.Errorf("driver.Transactions: %w", err)
	}
	var driverRx = driver.TransactionResponseChannel(driverTx)

	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient())
	var committed = client.NewAsyncOperation()
	committed.Resolve(nil)

	return &Materialize{
		combiner:        nil,
		combinerBuilder: bindings.NewCombineBuilder(schemaIndex),
		committed:       committed,
		coordinator:     coordinator,
		deltaUpdates:    false, // Set by RestoreCheckpoint.
		driverRx:        driverRx,
		driverTx:        driverTx,
		flighted:        make(map[string]json.RawMessage),
		spec:            spec,
		readBuilder:     readBuilder,
		recorder:        recorder,
		request:         nil,
		store:           store,
	}, nil
}

// Implementing consumer.Store for Materialize
var _ consumer.Store = (*Materialize)(nil)

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	// Write our intent to close the transaction and prepare for commit.
	// This signals the driver to send remaining Loaded responses, if any.
	if err := lifecycle.WritePrepare(m.driverTx, &m.request, checkpoint); err != nil {
		return client.FinishedOperation(fmt.Errorf("sending Prepare: %w", err))
	}

	// Drain remaining Loaded responses into the *Combiner, until we read Prepared.
	for {
		var next = <-m.driverRx
		if next.Error != nil {
			return client.FinishedOperation(fmt.Errorf(
				"reading Loaded or Prepared: %w", next.Error))
		} else if next.Loaded != nil {
			// Feed documents into the combiner as reduce-left operations.
			for _, slice := range next.Loaded.DocsJson {
				if err := m.combiner.ReduceLeft(next.Loaded.Arena.Bytes(slice)); err != nil {
					return client.FinishedOperation(fmt.Errorf("combiner.ReduceLeft: %w", err))
				}
			}
		} else if next.Prepared != nil {
			// Stage a provided driver checkpoint to commit with this transaction.
			if next.Prepared.DriverCheckpoint != nil {
				m.store.State.(*storeState).DriverCheckpoint = next.Prepared.DriverCheckpoint
			}
			break // All done.
		} else {
			// Protocol error.
			return client.FinishedOperation(fmt.Errorf(
				"expected Loaded or Prepared, got %#v",
				next.TransactionResponse.String(),
			))
		}
	}

	// Precondition: |m.flighted| contains the precise set of keys in this transaction.
	// See FinalizeTxn.
	var remaining = len(m.flighted)

	// Drain the combiner.
	if err := m.combiner.Finish(func(full bool, docRaw json.RawMessage, packedKey, packedValues []byte) error {
		// Inlined use of string(packedKey) clues compiler escape analysis to avoid allocation.
		if _, ok := m.flighted[string(packedKey)]; !ok {
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

		if err := lifecycle.StageStore(m.driverTx, &m.request,
			packedKey, packedValues, docRaw, full,
		); err != nil {
			return err
		}

		// We can retain a bounded number of documents from this transaction
		// as a performance optimization, so that they may be directly available
		// to the next transaction without issuing a Load.
		if m.deltaUpdates || remaining >= cachedDocumentBound {
			delete(m.flighted, string(packedKey)) // Don't retain.
		} else {
			// We cannot reference |rawDoc| beyond this callback, and must copy.
			// Fortunately, StageStore did just that, appending the document
			// to the staged request Arena, which we can reference here because
			// Arena bytes are write-once.
			var s = m.request.Store
			m.flighted[string(packedKey)] = s.Arena.Bytes(s.DocsJson[len(s.DocsJson)-1])
		}

		remaining--
		return nil

	}); err != nil {
		return client.FinishedOperation(fmt.Errorf("combine.Finish: %w", err))
	}

	m.combinerBuilder.Release(m.combiner)
	m.combiner = nil

	// We should have seen 1:1 combined documents for each flighted key.
	if remaining != 0 {
		log.WithFields(log.Fields{
			"remaining": remaining,
			"flighted":  len(m.flighted),
		}).Panic("combiner drained, but expected documents remainder != 0")
	}

	// Wait for any |waitFor| operations. In practice this is always empty.
	// It would contain pending journal writes, but materializations don't issue any.
	for op := range waitFor {
		if op.Err() != nil {
			return client.FinishedOperation(fmt.Errorf("dependency failed: %w", op.Err()))
		}
	}

	if err := lifecycle.WriteCommit(m.driverTx, &m.request); err != nil {
		return client.FinishedOperation(err)
	}

	// Spawn a task which awaits the Committed response (rather than blocking to wait).
	// This optimistically pipelines the next transaction while the store commits this one.
	m.committed = client.NewAsyncOperation()
	go awaitCommitted(m.driverRx, m.committed)

	// Tell our JSON store to commit to its recovery log after |m.committed| resolves.
	return m.store.StartCommit(shard, checkpoint, consumer.OpFutures{m.committed: struct{}{}})
}

func awaitCommitted(driverRx <-chan driver.TransactionResponse, result *client.AsyncOperation) {
	var m = <-driverRx

	if m.Error != nil {
		result.Resolve(fmt.Errorf("reading Committed: %w", m.Error))
	} else if m.Committed == nil {
		result.Resolve(fmt.Errorf("expected Committed, got %#v", m.TransactionResponse))
	} else {
		result.Resolve(nil)
	}
}

// RestoreCheckpoint implements consumer.Store.RestoreCheckpoint
func (m *Materialize) RestoreCheckpoint(shard consumer.Shard) (checkpoint pc.Checkpoint, err error) {
	checkpoint, err = m.store.RestoreCheckpoint(shard)
	if err != nil {
		err = fmt.Errorf("store.RestoreCheckpoint: %w", err)
		return
	}

	if err = lifecycle.WriteOpen(
		m.driverTx,
		&m.request,
		m.spec.EndpointType,
		m.spec.EndpointConfig,
		m.spec.FieldSelection,
		shard.FQN(),
		m.store.State.(*storeState).DriverCheckpoint,
	); err != nil {
		return
	}

	var opened = <-m.driverRx
	if opened.Error != nil {
		err = fmt.Errorf("reading Opened: %w", opened.Error)
		return
	} else if opened.Opened == nil {
		err = fmt.Errorf("expected Opened, got %#v",
			opened.TransactionResponse.String())
		return
	}

	// If the store provided a Flow checkpoint, prefer that over
	// the |checkpoint| recovered from the store.
	if b := opened.Opened.FlowCheckpoint; len(b) != 0 {
		checkpoint = pc.Checkpoint{}
		if err = checkpoint.Unmarshal(b); err != nil {
			err = fmt.Errorf("unmarshal Opened.FlowCheckpoint: %w", err)
			return
		}
	}
	m.deltaUpdates = opened.Opened.DeltaUpdates

	return checkpoint, nil
}

// Destroy implements consumer.Store.Destroy
func (m *Materialize) Destroy() {
	m.store.Destroy()
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

// Coordinator implements shuffle.Store.Coordinator
func (m *Materialize) Coordinator() *shuffle.Coordinator {
	return m.coordinator
}

// Implementing runtime.Application for Materialize
var _ Application = (*Materialize)(nil)

// BeginTxn implements Application.BeginTxn
func (m *Materialize) BeginTxn(shard consumer.Shard) error {

	var err error
	m.combiner, err = m.combinerBuilder.Open(
		m.spec.Collection.SchemaUri,
		m.spec.Collection.KeyPtrs,
		m.spec.FieldValuePtrs(),
		"", // Don't generate UUID placeholders.
	)
	if err != nil {
		return fmt.Errorf("building combiner: %w", err)
	}

	return nil
}

// pollLoaded selects and processes Loaded responses which can be read without blocking.
func (m *Materialize) pollLoaded() error {
	for {
		var resp driver.TransactionResponse
		select {
		case resp = <-m.driverRx:
		default:
			return nil
		}

		if resp.Error != nil {
			return fmt.Errorf("reading Loaded: %w", resp.Error)
		} else if resp.Loaded != nil {
			// Feed documents into the combiner as reduce-left operations.
			for _, slice := range resp.Loaded.DocsJson {
				if err := m.combiner.ReduceLeft(resp.Loaded.Arena.Bytes(slice)); err != nil {
					return fmt.Errorf("combiner.ReduceLeft: %w", err)
				}
			}
		} else {
			return fmt.Errorf("expected Loaded, got %#v", resp.TransactionResponse)
		}
	}
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

	if doc, ok := m.flighted[string(packedKey)]; ok && doc == nil {
		// We've already seen this key within this transaction.
	} else if ok {
		// We retained this document from the last transaction.
		if m.deltaUpdates {
			panic("we shouldn't have retained if deltaUpdates")
		}
		if err := m.combiner.ReduceLeft(doc); err != nil {
			return fmt.Errorf("combiner.ReduceLeft: %w", err)
		}
		m.flighted[string(packedKey)] = nil // Clear old value & mark as visited.
	} else {
		// This is a novel key.
		if !m.deltaUpdates {
			if err := lifecycle.StageLoad(m.driverTx, &m.request, packedKey); err != nil {
				return err
			}
		}
		m.flighted[string(packedKey)] = nil // Mark as visited.
	}

	if err := m.combiner.CombineRight(doc.Arena.Bytes(doc.DocsJson[doc.Index])); err != nil {
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
	for key, doc := range m.flighted {
		if doc != nil {
			delete(m.flighted, key)
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

// StartReadingMessages implements Application.StartReadingMessages
func (m *Materialize) StartReadingMessages(shard consumer.Shard, checkpoint pc.Checkpoint, tp *flow.Timepoint, channel chan<- consumer.EnvelopeOrError) {
	log.WithFields(log.Fields{
		"shard":      shard.Spec().Labels,
		"checkpoint": checkpoint,
	}).Debug("Starting to Read Messages")
	shuffle.StartReadingMessages(shard.Context(), m.readBuilder, checkpoint, tp, channel)
}

// ReadThrough delegates to shuffle.ReadThrough
func (m *Materialize) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	return m.readBuilder.ReadThrough(offsets)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (m *Materialize) ReplayRange(shard consumer.Shard, journal pb.Journal, begin pb.Offset, end pb.Offset) message.Iterator {
	return m.readBuilder.StartReplayRead(shard.Context(), journal, begin, end)
}

// TODO(johnny): This is an interesting knob that should be exposed.
const cachedDocumentBound = 2048
