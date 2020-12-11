package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// MaterializeTransaction wraps the transaction from the target system and also holds the Combine
// stream and the set of keys that have been observed in this transaction.
type MaterializeTransaction struct {
	storeTxn  materialize.TargetTransaction
	combine   *flow.Combine
	keyCounts map[string]int
}

// Materialize is an Application implementation that materializes a view of a collection into a
// target database. The name of the collection and materialization are taken from labels on the
// Shard. This delegates to a MaterializationTarget, which implements the consumer.Store interface,
// for all of the communication with the remote system.
type Materialize struct {
	name           string
	delegate       *flow.WorkerHost
	readBuilder    *shuffle.ReadBuilder
	coordinator    *shuffle.Coordinator
	collectionSpec *pf.CollectionSpec
	targetStore    materialize.Target
	txn            *MaterializeTransaction
}

// NewMaterializeApp returns a new Materialize, which implements Application
func NewMaterializeApp(
	service *consumer.Service,
	journals *keyspace.KeySpace,
	shard consumer.Shard,
	_ *recoverylog.Recorder,
) (*Materialize, error) {
	log.Infof("Initializing Materialization for %v", shard.Spec().Labels)
	var catalogURL, err = shardLabel(shard, labels.CatalogURL)
	if err != nil {
		return nil, err
	}
	collectionName, err := shardLabel(shard, labels.Collection)
	if err != nil {
		return nil, err
	}
	targetName, err := shardLabel(shard, labels.MaterializationTarget)
	if err != nil {
		return nil, err
	}
	tableName, err := shardLabel(shard, labels.MaterializationTableName)
	if err != nil {
		return nil, err
	}

	// We don't use a recovery log for materializations, since their checkpoints are stored in the
	// target system. Passing the empty string here has the effect of just using a temp file for the
	// catalog. Note that we only read basic information from the catalog like the connection info
	// for the target system and the table name. Specifics about the set of projections will come
	// from the target system itself.
	catalog, err := flow.NewCatalog(catalogURL, "")
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}

	collectionSpec, err := catalog.LoadCollection(collectionName)
	if err != nil {
		return nil, fmt.Errorf("loading collection spec: %w", err)
	}
	targetSpec, err := catalog.LoadMaterializationTarget(targetName)
	if err != nil {
		return nil, fmt.Errorf("loading materialization spec: %w", err)
	}
	targetSpec.TableName = tableName

	err = catalog.Close()
	if err != nil {
		return nil, fmt.Errorf("closing catalog database: %w", err)
	}

	// Initialize the Store implementation for the target system. This will actually connect to the
	// target system and initialize the set of projected fields from data stored there.
	targetStore, err := materialize.NewMaterializationTarget(targetSpec)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize materialization from target database: %w", err)
	}

	readerSpec := pf.ReadSpec{
		SourceName: collectionName,
		SourcePartitions: pb.LabelSelector{
			Include: pb.MustLabelSet(labels.Collection, collectionName),
		},
		Shuffle: pf.Shuffle{
			ShuffleKeyPtr: collectionSpec.KeyPtrs,
			UsesSourceKey: true,
		},
		ReaderType:        "materialization",
		ReaderNames:       []string{collectionName, targetName, tableName},
		ReaderCatalogDBID: targetSpec.CatalogDBID,
	}
	readBuilder, err := shuffle.NewReadBuilder(service, journals, shard, []pf.ReadSpec{readerSpec})
	if err != nil {
		return nil, fmt.Errorf("NewReadBuilder: %w", err)
	}

	delegate, err := flow.NewWorkerHost(
		"combine",
		"--catalog",
		catalogURL,
	)
	if err != nil {
		return nil, fmt.Errorf("starting materialization flow-worker: %w", err)
	}

	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient())

	log.WithFields(log.Fields{
		"collection":            collectionName,
		"materializationTarget": targetName,
		"tableName":             tableName,
	}).Info("Successfully initialized materialization")

	return &Materialize{
		delegate:       delegate,
		readBuilder:    readBuilder,
		coordinator:    coordinator,
		collectionSpec: &collectionSpec,
		targetStore:    targetStore,
		txn:            nil,
	}, nil
}

// Implementing consumer.Store for Materialize
var _ consumer.Store = (*Materialize)(nil)

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	return m.targetStore.StartCommit(shard, checkpoint, waitFor)
}

// RestoreCheckpoint implements consumer.Store.RestoreCheckpoint
func (m *Materialize) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	return m.targetStore.RestoreCheckpoint(shard)
}

// Destroy implements consumer.Store.Destroy
func (m *Materialize) Destroy() {
	// `self` will be null if the initialization returned an error, so we check here to avoid
	// polluting the logs.
	if m != nil {
		m.targetStore.Destroy()
	}
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

// Coordinator implements shuffle.Store.Coordinator
func (m *Materialize) Coordinator() *shuffle.Coordinator {
	return m.coordinator
}

// Implementing runtime.Application for Materialize
var _ Application = (*Materialize)(nil)

// BuildHints implements Application.BuildHints
func (m *Materialize) BuildHints() (recoverylog.FSMHints, error) {
	// This is a no-op since we aren't using a recover log
	return recoverylog.FSMHints{}, nil
}

// BeginTxn implements Application.BeginTxn
func (m *Materialize) BeginTxn(shard consumer.Shard) error {
	if m.txn != nil {
		return fmt.Errorf("BeginTxn called while a transaction was already in progress")
	}
	log.WithFields(log.Fields{
		"collection":      m.collectionSpec.Name.String(),
		"materialization": m.name,
	}).Debug("Starting new transaction")
	tx, err := m.targetStore.BeginTxn(shard.Context())
	if err != nil {
		return err
	}

	combine, err := flow.NewCombine(shard.Context(), pf.NewCombineClient(m.delegate.Conn), m.collectionSpec)
	if err != nil {
		return err
	}
	// Our Combine RPCs should prune because, by construction, we ensure the
	// root-most document (the current DB row) is ordered first in the RPC.
	// This would *not* carry over to materializations into streams.
	const prune = true

	if err = combine.Open(m.targetStore.ProjectionPointers(), prune); err != nil {
		return fmt.Errorf("while sending RPC open %q: %w", m.collectionSpec.Name, err)
	}
	m.txn = &MaterializeTransaction{
		storeTxn:  tx,
		combine:   combine,
		keyCounts: make(map[string]int),
	}
	return nil
}

// ConsumeMessage implements Application.ConsumeMessage
func (m *Materialize) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	if m.txn == nil {
		panic("ConsumeMessage called with nil transaction")
	}

	var doc = envelope.Message.(pf.IndexedShuffleResponse)

	var flags = message.GetFlags(doc.GetUUID())
	if flags == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	log.WithFields(log.Fields{
		"collection":      m.collectionSpec.Name.String(),
		"materialization": m.name,
		"messageUuid":     envelope.GetUUID(),
	}).Debug("on ConsumeMessage")

	var key = doc.Arena.Bytes(doc.PackedKey[doc.Index])

	// We need to check if we've added the existing document to the Combine already. If not,
	// then we'll fetch the existing document (either from cache or the materialization
	// database) and add that to the Combine. The "packed" shuffle key is used to key the cache
	// and map of observed documents.
	// NOTE: use string(key) to avoid allocation if the map key already exists.
	if _, isPresent := m.txn.keyCounts[string(key)]; !isPresent {
		var keyTuple, err = tuple.Unpack(key)
		if err != nil {
			return fmt.Errorf("failed to unpack key tuple: %w", err)
		}

		var keyIface = make([]interface{}, len(keyTuple))
		for i := range keyTuple {
			keyIface[i] = keyTuple[i]
		}

		fetched, err := m.txn.storeTxn.FetchExistingDocument(keyIface)
		if err != nil {
			return fmt.Errorf("Failed to fetch existing document (key %v): %w", keyTuple, err)
		}

		if len(fetched) > 0 {
			if err = m.txn.combine.Add(fetched); err != nil {
				return fmt.Errorf("Failed to add existing document to combine RPC: %w", err)
			}
		}
	}
	m.txn.keyCounts[string(key)]++

	var docBytes = json.RawMessage(doc.Arena.Bytes(doc.DocsJson[doc.Index]))
	if err := m.txn.combine.Add(json.RawMessage(docBytes)); err != nil {
		return fmt.Errorf("Failed to add new document to combine RPC: %w", err)
	}

	return nil
}

// FinalizeTxn implements Application.FinalizeTxn
func (m *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	if m.txn == nil {
		return fmt.Errorf("FinalizeTxn called without any transaction in progress")
	}
	var totalKeys int
	var totalDocuments int
	for _, v := range m.txn.keyCounts {
		totalKeys++
		totalDocuments += v
	}
	log.WithFields(log.Fields{
		"shard":             shard.Spec().Labels,
		"observedKeys":      totalKeys,
		"observedDocuments": totalDocuments,
	}).Debug("on FinalizeTxn")

	if err := m.txn.combine.CloseSend(); err != nil {
		return fmt.Errorf("Failed to flush Combine RPC: %w", err)
	}
	return m.txn.combine.Finish(m.updateDatabase)
}

// FinishedTxn implements Application.FinishedTxn
func (m *Materialize) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {
	log.WithFields(log.Fields{
		"shard": shard.Spec().Labels,
	}).Debug("on FinishedTxn")

	// TODO(johnny): Block for commit of this transaction, before we start the next.
	// This is a dirty, dirty hack to avoid issues with the serialization of
	// otherwise pipelined transactions.
	<-op.Done()

	m.txn = nil
}

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

// ClearRegisters returns a "not implemented" error.
func (m *Materialize) ClearRegisters(context.Context, *pf.ClearRegistersRequest) (*pf.ClearRegistersResponse, error) {
	return new(pf.ClearRegistersResponse), fmt.Errorf("not implemented")
}

// Called for each document in the Combine RPC response, after all documents have been added for
// this transaction.
func (m *Materialize) updateDatabase(icr pf.IndexedCombineResponse) error {
	docIndex := icr.Index
	log.WithFields(log.Fields{
		"collection":      m.collectionSpec.Name.String(),
		"materialization": m.name,
		"docIndex":        icr.Index,
	}).Debug("Updating database")
	extractedFields, err := extractFields(docIndex, icr.Fields, icr.Arena)
	if err != nil {
		return err
	}

	// The full document json is always the last column, so we add that to the fields that were
	// extracted. This is all dependent on the order
	var documentJSON = icr.Arena.Bytes(icr.DocsJson[docIndex])

	err = m.txn.storeTxn.Store(extractedFields, documentJSON)
	if err != nil {
		return fmt.Errorf("Failed to store document: %w", err)
	}
	log.Debugf("Successfully updated database for document %d", docIndex)

	return nil
}

func extractFields(documentIndex int, fields []pf.Field, arena pf.Arena) ([]interface{}, error) {
	extractedFields := make([]interface{}, len(fields))
	for i, field := range fields {
		extractedValue, err := getValue(field.Values[documentIndex], arena)
		if err != nil {
			return nil, err
		}
		extractedFields[i] = extractedValue
	}
	return extractedFields, nil
}

// Safe version that returns the value of a field. Copies contents out of the arena, if necessary.
func getValue(field pf.Field_Value, arena pf.Arena) (interface{}, error) {
	switch field.Kind {
	case pf.Field_Value_NULL:
		return nil, nil
	case pf.Field_Value_TRUE:
		return true, nil
	case pf.Field_Value_FALSE:
		return false, nil
	case pf.Field_Value_UNSIGNED:
		return field.Unsigned, nil
	case pf.Field_Value_SIGNED:
		return field.Signed, nil
	case pf.Field_Value_DOUBLE:
		return field.Double, nil
	case pf.Field_Value_OBJECT, pf.Field_Value_ARRAY, pf.Field_Value_STRING:
		bytes := arena[field.Bytes.Begin:field.Bytes.End]
		return string(bytes), nil
	default:
		return nil, fmt.Errorf("invalid field value: %#v", field)
	}
}
