package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/materialize"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/estuary/flow/go/shuffle"
	cache "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// Wraps the transaction from the target system and also holds the Combine stream
// and the set of keys that have been observed in this transaction.
type MaterializeTransaction struct {
	storeTransaction           materialize.TargetTransaction
	combine                    *flow.Combine
	observedDocumentPackedKeys map[string]int
}

// An Application implementation that materializes a view of a collection into a target database.
// The name of the collection and materialization are taken from labels on the Shard.
// This delegates to a MaterializationTarget, which implements the consumer.Store interface, for
// all of the communication with the remote system.
type Materialize struct {
	materializationName string
	delegate            *flow.WorkerHost
	readBuilder         *shuffle.ReadBuilder
	coordinator         *shuffle.Coordinator
	collectionSpec      *pf.CollectionSpec
	documentCache       *cache.Cache
	targetStore         materialize.Target
	transacton          *MaterializeTransaction
}

func NewMaterializeApp(
	service *consumer.Service,
	journals *keyspace.KeySpace,
	extractor *flow.WorkerHost,
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
		return nil, fmt.Errorf("Failed to initialize matarialization from target database: %w", err)
	}

	readerSpec := pf.ReadSpec{
		SourceName: collectionName,
		Shuffle: pf.Shuffle{
			ShuffleKeyPtr: collectionSpec.KeyPtrs,
			UsesSourceKey: true,
		},
		ReaderType:        "materialization",
		ReaderNames:       []string{collectionName, targetName, tableName},
		ReaderCatalogDbId: targetSpec.CatalogDbId,
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

	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient(),
		pf.NewExtractClient(extractor.Conn))

	// There's lots of room to optimize the size/characteristics of the cache, but we're ignoring all
	// that for now and just using a reasonable limit on the total number of entries.
	cache, err := cache.New(1024)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize materialization document cache: %w", err)
	}
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
		documentCache:  cache,
		targetStore:    targetStore,
		transacton:     nil,
	}, nil
}

// Implementing consumer.Store for Materialize
var _ consumer.Store = (*Materialize)(nil)

// These functions just get delegated to the Store implementation for the target system
func (self *Materialize) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	return self.targetStore.StartCommit(shard, checkpoint, waitFor)
}

func (self *Materialize) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	return self.targetStore.RestoreCheckpoint(shard)
}

func (self *Materialize) Destroy() {
	// `self` will be null if the initialization returned an error, so we check here to avoid
	// polluting the logs.
	if self != nil {
		self.targetStore.Destroy()
	}
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

func (self *Materialize) Coordinator() *shuffle.Coordinator {
	return self.coordinator
}

// Implementing runtime.Application for Materialize
var _ Application = (*Materialize)(nil)

func (self *Materialize) BuildHints() (recoverylog.FSMHints, error) {
	// This is a no-op since we aren't using a recover log
	return recoverylog.FSMHints{}, nil
}

func (self *Materialize) BeginTxn(shard consumer.Shard) error {
	if self.transacton != nil {
		return fmt.Errorf("BeginTxn called while a transaction was already in progress")
	}
	log.WithFields(log.Fields{
		"collection":      self.collectionSpec.Name.String(),
		"materialization": self.materializationName,
	}).Debug("Starting new transaction")
	tx, err := self.targetStore.BeginTxn(shard.Context())
	if err != nil {
		return err
	}

	combine, err := flow.NewCombine(shard.Context(), pf.NewCombineClient(self.delegate.Conn), self.collectionSpec)
	if err != nil {
		return err
	}
	err = combine.Open(self.targetStore.ProjectionPointers())
	if err != nil {
		return err
	}
	self.transacton = &MaterializeTransaction{
		storeTransaction:           tx,
		combine:                    combine,
		observedDocumentPackedKeys: make(map[string]int),
	}
	return nil
}

func (self *Materialize) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	if self.transacton == nil {
		return fmt.Errorf("ConsumeMessage called without any transaction in progress")
	}

	shuffleResponse := envelope.Message.(pf.IndexedShuffleResponse)
	if len(shuffleResponse.TerminalError) > 0 {
		return fmt.Errorf("Terminal Error on shuffled read: %s", shuffleResponse.TerminalError)
	}

	var flags = message.GetFlags(shuffleResponse.GetUUID())
	if flags == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	log.WithFields(log.Fields{
		"collection":      self.collectionSpec.Name.String(),
		"materialization": self.materializationName,
		"messageUuid":     envelope.GetUUID(),
	}).Debug("on ConsumeMessage")

	packedShuffleKey := extractPackedKey(shuffleResponse)

	// We need to check if we've added the existing document to the Combine already. If not,
	// then we'll fetch the existing document (either from cache or the materialization
	// database) and add that to the Combine. The "packed" shuffle key, represented as a string,
	// is used as the key for the cache and the hashmap of ovserved document ids. This is
	// because go doesn't allow `[]interface{}` to be used as a map key.
	if _, isPresent := self.transacton.observedDocumentPackedKeys[packedShuffleKey]; !isPresent {
		primaryKeys, err := unsafeExtractFields(shuffleResponse.Index, shuffleResponse.ShuffleKey, shuffleResponse.Arena)
		if err != nil {
			return fmt.Errorf("Failed to extract primary keys from document: %w", err)
		}
		existingDocument, err := self.fetchExistingDocument(packedShuffleKey, primaryKeys)
		if err != nil {
			return fmt.Errorf("Failed to fetch existing document for keys: %v: %w", primaryKeys, err)
		}
		if len(existingDocument) > 0 {
			err = self.transacton.combine.Add(existingDocument)
			if err != nil {
				return fmt.Errorf("Failed to add existing document to combine RPC: %w", err)
			}
		}
	}
	self.transacton.observedDocumentPackedKeys[packedShuffleKey] += 1

	sliceRange := shuffleResponse.DocsJson[shuffleResponse.Index]
	bytes := shuffleResponse.Arena.Bytes(sliceRange)
	err := self.transacton.combine.Add(json.RawMessage(bytes))
	if err != nil {
		return fmt.Errorf("Failed to add new document to combine RPC: %w", err)
	}

	return nil
}

func (self *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	if self.transacton == nil {
		return fmt.Errorf("FinalizeTxn called without any transaction in progress")
	}
	var totalKeys int
	var totalDocuments int
	for _, v := range self.transacton.observedDocumentPackedKeys {
		totalKeys += 1
		totalDocuments += v
	}
	log.WithFields(log.Fields{
		"shard":             shard.Spec().Labels,
		"observedKeys":      totalKeys,
		"observedDocuments": totalDocuments,
	}).Debug("on FinalizeTxn")

	if err := self.transacton.combine.CloseSend(); err != nil {
		return fmt.Errorf("Failed to flush Combine RPC: %w", err)
	}
	return self.transacton.combine.Finish(self.updateDatabase)
}

func (self *Materialize) FinishedTxn(shard consumer.Shard, _ consumer.OpFuture) {
	log.WithFields(log.Fields{
		"shard": shard.Spec().Labels,
	}).Debug("on FinishedTxn")
	self.transacton = nil
}

func (self *Materialize) StartReadingMessages(shard consumer.Shard, checkpoint pc.Checkpoint, tp *flow.Timepoint, channel chan<- consumer.EnvelopeOrError) {
	log.WithFields(log.Fields{
		"shard":      shard.Spec().Labels,
		"checkpoint": checkpoint,
	}).Debug("Starting to Read Messages")
	shuffle.StartReadingMessages(shard.Context(), self.readBuilder, checkpoint, tp, channel)
}

// ReadThrough delegates to shuffle.ReadThrough
func (self *Materialize) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	return self.readBuilder.ReadThrough(offsets)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (self *Materialize) ReplayRange(shard consumer.Shard, journal pb.Journal, begin pb.Offset, end pb.Offset) message.Iterator {
	return self.readBuilder.StartReplayRead(shard.Context(), journal, begin, end)
}

// ClearRegisters returns a "not implemented" error.
func (m *Materialize) ClearRegisters(context.Context, *pf.ClearRegistersRequest) (*pf.ClearRegistersResponse, error) {
	return new(pf.ClearRegistersResponse), fmt.Errorf("not implemented")
}

// Called for each document in the Combine RPC response, after all documents have been added for
// this transaction.
func (self *Materialize) updateDatabase(icr pf.IndexedCombineResponse) error {
	docIndex := icr.Index
	log.WithFields(log.Fields{
		"collection":      self.collectionSpec.Name.String(),
		"materialization": self.materializationName,
		"docIndex":        icr.Index,
	}).Debug("Updating database")
	extractedFields, err := unsafeExtractFields(docIndex, icr.Fields, icr.Arena)
	if err != nil {
		return err
	}

	// The full document json is always the last column, so we add that to the fields that were
	// extracted. This is all dependent on the order
	var documentJson = icr.Arena.Bytes(icr.DocsJson[docIndex])

	err = self.transacton.storeTransaction.Store(extractedFields, documentJson)
	if err != nil {
		return fmt.Errorf("Failed to store document: %w", err)
	}
	log.Debugf("Successfully updated database for document %d", docIndex)

	packedKey := self.getPackedKey(icr)
	self.documentCache.Add(packedKey, json.RawMessage(documentJson))
	return nil
}

func (self *Materialize) fetchExistingDocument(packedPrimaryKey string, primaryKeys []interface{}) (json.RawMessage, error) {
	var documentJson json.RawMessage
	var rawDocument, exists = self.documentCache.Get(packedPrimaryKey)
	if exists {
		documentJson = rawDocument.(json.RawMessage)
	} else {
		var fetched, err = self.transacton.storeTransaction.FetchExistingDocument(primaryKeys)
		if err != nil {
			return nil, fmt.Errorf("Failed to retrieve existing document: %w", err)
		}
		documentJson = fetched
	}
	return documentJson, nil
}

func (self *Materialize) getPackedKey(icr pf.IndexedCombineResponse) string {
	var packedBytes []byte
	for _, i := range self.targetStore.PrimaryKeyFieldIndexes() {
		icr.Fields[i].Values[icr.Index].EncodePacked(packedBytes, icr.Arena)
	}
	return string(packedBytes)
}

func extractPackedKey(shuffleResponse pf.IndexedShuffleResponse) string {
	byteRange := shuffleResponse.PackedKey[shuffleResponse.Index]
	keyBytes := shuffleResponse.Arena[byteRange.Begin:byteRange.End]
	return string(keyBytes)
}

func unsafeExtractFields(documentIndex int, fields []pf.Field, arena pf.Arena) ([]interface{}, error) {
	extractedFields := make([]interface{}, len(fields))
	for i, field := range fields {
		extractedValue, err := unsafeGetValue(field.Values[documentIndex], arena)
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

// Returns the field value, which may still contain a pointer into the arena.
func unsafeGetValue(field pf.Field_Value, arena pf.Arena) (interface{}, error) {
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
		return unsafeGetString(field.Bytes, arena), nil
	default:
		return nil, fmt.Errorf("invalid field value: %#v", field)
	}
}

func unsafeGetString(slice pf.Slice, arena pf.Arena) string {
	bytes := arena[slice.Begin:slice.End]
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}
