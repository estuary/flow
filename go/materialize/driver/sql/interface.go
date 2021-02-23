package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"

	// Below are imports needed by the go sql package. These are not used directly, but they are
	// required in order to connect to the databases.
	// The sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

// Transaction is returned from a call to start a transaction.
type Transaction struct {
	// LoadKeyCh is a channel into which keys to load are written.
	// It's closed when all load keys have been sent, and at that time
	// the Transaction must begin sending loaded documents if it hasn't already.
	LoadKeyCh chan<- tuple.Tuple

	// LoadedDocumentCh is a channel into which the transaction implementation
	// sends loaded documents, either as they're processed from LoadKeyCh,
	// or all at once upon reading a LoadKeyCh close. Once all loaded documents
	// have been sent, the channel is closed to indicate so to the driver.
	// A non-nil Error is to be considered terminal and aborts the Transaction.
	LoadedDocumentCh <-chan LoadedDocument

	// StoreDocumentCh is a channel into which StoreDocuments are written
	// by the driver. The implementation is expected to begin processing StoreDocuments
	// only after a successful close of LoadedDocumentCh.
	// The channel is closed by the driver once all StoreDocuments have been sent,
	// indicating the Transaction should commit.
	StoreDocumentCh chan<- StoreDocument

	// CommitCh is a channel into which a single, final commit status error is sent.
	// The implementation is expected to send a final nil or error after StoreDocumentCh
	// has closed and the Transaction has fully committed, or failed to commit.
	CommitCh <-chan error
}

// LoadedDocument returns the result of loading a document. One of Error or Document must be set. No
// LoadedDocument should be sent for a document that was not found.
type LoadedDocument struct {
	// Error is an error that occurred during the loading of documents. This will be considered a
	// terminal error that aborts the transaction.
	Error error
	// Document is the result of a successful query for a document.
	Document json.RawMessage
	// .. can be extended with key & value fields, etc in the future.
}

// StoreDocument represents a document to be stored.
type StoreDocument struct {
	// Update will be true if this document was previously loaded successfully.
	Update bool
	// Key is the extracted values of the key
	Key tuple.Tuple
	// Values holds all other projected that aren't part of the collection's key.
	Values tuple.Tuple
	// Document is the full json to store
	Document json.RawMessage
	// Commit is true only for the final StoreDocument, and only if no other errors have been
	// encountered during the transaction. This represents an instruction for the transaction to
	// begin committing and send the result on the CommitCh. If Commit is true, then all other
	// values will be zeroed. A Document will never be sent in the same instance as a Commit.
	Commit bool
}

// Connection represents a pool of database connections for a specific URI.
type Connection interface {
	// StartTransaction starts a new database transaction, in which all load and store operations
	// will be executed. The `flowCheckpoint` must be updated within this transaction, though the
	// actual timing of it can be whatever makes sense for the database/driver. For most sql
	// databases, we can just update the checkpoint at the beginning of the transaction, which
	// allows us to fail faster if the nonce does not match.
	StartTransaction(ctx context.Context, handle *Handle, flowCheckpoint []byte, cachedSQL *CachedSQL) (Transaction, error)
	// QueryMaterializationSpec returns the materializetion spec associated with the given Handle,
	// or nil if the spec does not exist.
	QueryMaterializationSpec(ctx context.Context, handle *Handle) (*MaterializationSpec, error)
	// Fence executes the Fence rpc. It's difficult to factor out common logic for this rpc, so the
	// strategy is to give each implementation full control over how this happens. Fence must return
	// the most recently _committed_ flow checkpoint value, which may be an empty slice.
	Fence(ctx context.Context, handle *Handle) ([]byte, error)
	// GenerateRuntimeSQL creates the sql statements that will be cached and re-used for subsequent
	// transactions with the same nonce.
	GenerateRuntimeSQL(ctx context.Context, handle *Handle, spec *MaterializationSpec) (*CachedSQL, error)
	// GenerateApplyStatements creates all of the sql statements that are needed to apply the given
	// materialization spec. This function must not execute the statements or modify the database.
	GenerateApplyStatements(ctx context.Context, handle *Handle, spec *MaterializationSpec) ([]string, error)
	// ExecApplyStatements executes all of the given statements within a single transaction.
	ExecApplyStatements(ctx context.Context, handle *Handle, statements []string) error
}

// ConnectionBuilder hands out Connections for each Handle.
type ConnectionBuilder interface {
	// Connection returns a Connection for the given Handle.
	Connection(ctx context.Context, uri string) (Connection, error)
}

// CacheingConnectionManager caches each Connection, using the handle URI as a key. Currently, this
// cache is never invalidated, as each Connection is assumed to represent a pool of connections for
// a specific URI.
type CacheingConnectionManager struct {
	inner ConnectionBuilder
	// map of connection URI to DB for that system. We don't ever remove anythign from this map, so
	// it'll just keep growing if someone makes a bunch of requests for many distinct endpoints.
	connections      map[string]Connection
	connectionsMutex sync.Mutex
}

// NewCache returns a CacheingConnectionManager that wraps the given inner connectionManager and
// caches each returned Connection, using the URI as a key.
func NewCache(inner ConnectionBuilder) *CacheingConnectionManager {
	return &CacheingConnectionManager{
		inner:       inner,
		connections: make(map[string]Connection),
	}
}

// Connection implements the ConnectionManager interface
func (c *CacheingConnectionManager) Connection(ctx context.Context, uri string) (Connection, error) {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()

	if db, ok := c.connections[uri]; ok {
		return db, nil
	}

	var db, err = c.inner.Connection(ctx, uri)
	if err != nil {
		return nil, err
	}
	c.connections[uri] = db
	return db, nil
}

// A GenericDriver implements the DriverServer interface using the Connection, Transaction,
// ConnectionManager, and SQLGen interfaces.
type GenericDriver struct {
	ParseConfig func(json.RawMessage) (uri string, table string, err error)
	// The SQLGenerator to use for generating statements for use with the go sql package.
	SQLGen SQLGenerator
	// Connections creates and caches all connections.
	Connections *CacheingConnectionManager
	// SQLCache is a map of caller id to the sql that we cache.
	SQLCache      map[string]*CachedSQL
	sqlCacheMutex sync.Mutex
}

// StartSession is part of the DriverServer implementation.
func (g *GenericDriver) StartSession(ctx context.Context, req *pm.SessionRequest) (*pm.SessionResponse, error) {
	var uri, table, err = g.ParseConfig(json.RawMessage(req.EndpointConfigJson))
	if err != nil {
		return new(pm.SessionResponse), err
	}
	var handle = Handle{
		Nonce:   rand.Int31(),
		URI:     uri,
		Table:   table,
		ShardID: req.ShardId,
	}
	handleBytes, err := json.Marshal(handle)
	if err != nil {
		return nil, err
	}
	var response = new(pm.SessionResponse)
	response.Handle = handleBytes
	return response, nil
}

// Validate is part of the DriverServer implementation.
func (g *GenericDriver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}
	var proposed = req.Collection
	err = proposed.Validate()
	if err != nil {
		return nil, fmt.Errorf("The proposed CollectionSpec is invalid: %w", err)
	}

	conn, err := g.Connections.Connection(ctx, handle.URI)
	if err != nil {
		return nil, err
	}
	current, err := conn.QueryMaterializationSpec(ctx, handle)
	if err != nil {
		return nil, err
	}

	constraints, err := g.doValidate(ctx, handle, proposed, current)

	var response = new(pm.ValidateResponse)
	response.Constraints = constraints
	return response, nil
}

// Fence is part of the DriverServer implementation.
func (g *GenericDriver) Fence(ctx context.Context, req *pm.FenceRequest) (*pm.FenceResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}
	connection, err := g.Connections.Connection(ctx, handle.URI)
	if err != nil {
		return nil, err
	}
	flowCheckpoint, err := connection.Fence(ctx, handle)
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"shardId":              handle.ShardID,
		"nonce":                handle.Nonce,
		"flowCheckpointExists": len(flowCheckpoint) > 0,
	}).Infof("Fence executed successfully")
	return &pm.FenceResponse{
		FlowCheckpoint: flowCheckpoint,
	}, nil
}

func handleTxnRecvStream(logEntry *log.Entry, ctx context.Context, stream pm.Driver_TransactionServer, transaction Transaction, errorCh chan<- error) {
	var err = handleTxnRecvFailable(logEntry, ctx, stream, transaction)
	if err != nil {
		errorCh <- err
	}
	close(errorCh)
}

func handleTxnRecvFailable(logEntry *log.Entry, ctx context.Context, stream pm.Driver_TransactionServer, transaction Transaction) (retErr error) {
	defer func() {
		if retErr != nil {
			logEntry.WithField("error", retErr).Warnf("txRecv failed with err")
			if transaction.LoadKeyCh != nil {
				close(transaction.LoadKeyCh)
				transaction.LoadKeyCh = nil
			}
			if transaction.StoreDocumentCh != nil {
				close(transaction.StoreDocumentCh)
				transaction.StoreDocumentCh = nil
			}
		}
	}()
	var nLoadReqs, nDocs int
	for {
		var req, err = stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive next load message: %w", err)
		}
		if req.Load != nil {
			nLoadReqs++
			nDocs += len(req.Load.PackedKeys)
			logEntry.WithField("numDocs", len(req.Load.PackedKeys)).Trace("got load request")
			for _, key := range req.Load.PackedKeys {
				tup, err := tuple.Unpack(req.Load.Arena.Bytes(key))
				if err != nil {
					return err
				}
				select {
				case transaction.LoadKeyCh <- tup:
					// we sent the key
				case <-ctx.Done():
					// Something else has failed, so we should return
					return nil
				}
			}
		} else if req.LoadEOF != nil {
			break
		} else {
			return fmt.Errorf("expected either a Load or LoadEOF message")
		}
	}
	close(transaction.LoadKeyCh)
	transaction.LoadKeyCh = nil
	logEntry.WithFields(log.Fields{
		"totalLoadDocs": nDocs,
		"loadRequests":  nLoadReqs,
	}).Debug("Finished receiving LoadRequests")

	var nStoreReqs, nStoreDocs int
	for {
		var req, err = stream.Recv()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return fmt.Errorf("failed to receive next store message: %w", err)
		}
		if req.Store == nil {
			return fmt.Errorf("expected Store message")
		}
		nStoreReqs++
		nStoreDocs += len(req.Store.DocsJson)
		for i, docSlice := range req.Store.DocsJson {
			key, err := tuple.Unpack(req.Store.Arena.Bytes(req.Store.PackedKeys[i]))
			if err != nil {
				return err
			}
			values, err := tuple.Unpack(req.Store.Arena.Bytes(req.Store.PackedValues[i]))
			if err != nil {
				return err
			}
			var docJson = req.Store.Arena.Bytes(docSlice)
			var exists = req.Store.Exists[i]

			var store = StoreDocument{
				Update:   exists,
				Key:      key,
				Values:   values,
				Document: docJson,
			}
			select {
			case transaction.StoreDocumentCh <- store:
				// we sent the document
			case <-ctx.Done():
				// Something else has failed, so we should return
				return nil
			}
		}
	}
	logEntry.Debug("Sending instruction to commit")
	var commit = StoreDocument{
		Commit: true,
	}
	select {
	case transaction.StoreDocumentCh <- commit:
		// we sent the document
	case <-ctx.Done():
		// Something else has failed, so we should return
		return nil
	}
	close(transaction.StoreDocumentCh)
	transaction.StoreDocumentCh = nil
	logEntry.WithFields(log.Fields{
		"storeRequests":  nStoreReqs,
		"totalStoreDocs": nStoreDocs,
	}).Debug("Finished receiving StoreRequests")
	return nil
}

// LoadResponseArenaSize is the target size we'll try to hit for response arenas. The actual arena
// response sizes may be larger.
const LoadResponseArenaSize = 16 * 1024

func handleTxnSend(logEntry *log.Entry, ctx context.Context, stream pm.Driver_TransactionServer, transaction Transaction, errCh chan<- error) {
	var err = handleTxnSendFailable(logEntry, stream, transaction)
	errCh <- err
}

func handleTxnSendFailable(logEntry *log.Entry, stream pm.Driver_TransactionServer, transaction Transaction) error {
	// We'll re-use the same arena for all responses, just to avoid re-allocating in a tight loop.
	var loadArena = pf.Arena(make([]byte, 0, LoadResponseArenaSize))
	var slices = make([]pf.Slice, 0, 16)
	var nDocs = 0

	var doSend = func() error {
		logEntry.WithFields(log.Fields{
			"numDocs": nDocs,
		}).Debug("Sending LoadResponse")
		var resp = &pm.TransactionResponse{
			LoadResponse: &pm.TransactionResponse_LoadResponse{
				Arena:    loadArena,
				DocsJson: slices,
			},
		}
		var err = stream.Send(resp)
		// clear these for re-use
		loadArena = loadArena[:0]
		slices = slices[:0]
		nDocs = 0
		return err
	}

	for loaded := range transaction.LoadedDocumentCh {
		if loaded.Error != nil {
			return loaded.Error
		}
		if nDocs > 0 && len(loaded.Document)+len(loadArena) >= LoadResponseArenaSize {
			var err = doSend()
			if err != nil {
				return fmt.Errorf("failed to send LoadResponse")
			}
		}
		nDocs++
		slices = append(slices, loadArena.Add(loaded.Document))
	}
	// We may need to send the final LoadResponse here, since the arena may now be partially full.
	if len(slices) > 0 {
		var err = doSend()
		if err != nil {
			return fmt.Errorf("failed to send LoadResponse")
		}
	}

	var err = stream.Send(&pm.TransactionResponse{
		LoadEOF: &pm.LoadEOF{},
	})
	if err != nil {
		return err
	}
	logEntry.Debug("Finished sending LoadResponses")

	// Wait until we've committed, then send the response.
	err = <-transaction.CommitCh
	if err != nil {
		logEntry.WithField("error", err).Warn("Transaction Commit Failed")
		return err
	}
	logEntry.Debug("Transaction Commit Success")
	return stream.Send(&pm.TransactionResponse{
		StoreResponse: &pm.TransactionResponse_StoreResponse{},
	})
}

// Transaction is part of the DriverServer implementation
func (g *GenericDriver) Transaction(stream pm.Driver_TransactionServer) (retErr error) {
	var ctx = stream.Context()
	log.Debug("on Transaction start")
	req, err := stream.Recv()
	log.Debug("Received first message")
	if err != nil {
		return fmt.Errorf("Failed to receive Start message: %w", err)
	}
	if req.Start == nil {
		return fmt.Errorf("Expected Start message")
	}
	var start = req.Start

	handle, err := parseHandle(start.Handle)
	if err != nil {
		return err
	}
	var logEntry = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
	})
	logEntry.Debug("Starting transaction")

	conn, err := g.Connections.Connection(ctx, handle.URI)
	if err != nil {
		return err
	}

	cachedSQL, err := g.getCachedSQL(ctx, handle, conn)
	if err != nil {
		return err
	}

	logEntry.Trace("Starting Store transaction")
	transaction, err := conn.StartTransaction(ctx, handle, start.FlowCheckpoint, cachedSQL)
	if err != nil {
		return err
	}

	// The send and receive sides of this each run concurrently. An error could occur on either
	// side, and we want to return the first one.
	var sendCompleteCh = make(chan error)
	go handleTxnSend(logEntry, ctx, stream, transaction, sendCompleteCh)

	var recvCompleteCh = make(chan error)
	go handleTxnRecvStream(logEntry, ctx, stream, transaction, recvCompleteCh)

	select {
	case recvErr := <-recvCompleteCh:
		if recvErr != nil {
			return recvErr
		}
		var sendErr = <-sendCompleteCh
		if sendErr != nil {
			logEntry.WithField("error", sendErr).Warn("transaction failed")
			return sendErr
		}
		logEntry.Debug("Transaction completed successfully")
	case sendErr := <-sendCompleteCh:
		logEntry.Debugf("send completed before recv with error: %v", sendErr)
		if sendErr != nil {
			logEntry.WithField("error", sendErr).Warn("transaction send stream failed")
			return sendErr
		}
		var recvErr = <-recvCompleteCh
		if recvErr != nil {
			// We should never be able to reach this block because the send side should never
			// complete successfully unless the recv side has already complete successfully.
			logEntry.WithField("error", recvErr).Fatal("transaction recv stream failed after sendStream completed successfully")
		}
	}
	return nil
}

// Apply is part of the DriverServer implementation.
func (g *GenericDriver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}
	conn, err := g.Connections.Connection(ctx, handle.URI)
	if err != nil {
		return nil, err
	}

	// Has this materialization has already been applied?
	currentMaterialization, err := conn.QueryMaterializationSpec(ctx, handle)
	if err != nil {
		return nil, err
	}

	// Validate the request and determine the constraints, which will then be used to validate the
	// selected fields.
	constraints, err := g.doValidate(ctx, handle, req.Collection, currentMaterialization)
	if err != nil {
		return nil, err
	}
	// We don't handle any form of schema migrations, so we require that the list of
	// fields in the request is identical to the current fields. doValidate doesn't handle that
	// because the list of fields isn't known until Apply is called.
	if currentMaterialization != nil && !req.Fields.Equal(&currentMaterialization.Fields) {
		return nil, fmt.Errorf(
			"The set of fields in the request differs from the existing fields, which is disallowed because this driver does not perform schema migrations. Request fields: [%s], existing fields: [%s]",
			strings.Join(req.Fields.AllFields(), ", "),
			strings.Join(currentMaterialization.Fields.AllFields(), ", "),
		)
	}
	var logger = log.WithFields(log.Fields{
		"targetTable": handle.Table,
		"shardId":     handle.ShardID,
		"nonce":       handle.Nonce,
		"collection":  req.Collection.Collection,
	})

	// Only generate the sql if the current materialization spec is nil. If it's non-nil, then this
	// has already been applied.
	var response = new(pm.ApplyResponse)
	if currentMaterialization == nil {
		var materializationSpec = &MaterializationSpec{
			Collection: *req.Collection,
			Fields:     *req.Fields,
		}
		// Still validate the selected fields, even if this is just re-validating the existing
		// materializationSpec. The database could be modified manually, and we want to make sure to
		// surface errors if the spec is invalid.
		err = ValidateSelectedFields(constraints, materializationSpec)
		if err != nil {
			return nil, err
		}

		// Things look good, so it's time to generate all the DDL. We'll generate each statement
		// separately, since that's what we'll need in order to execute them. But each one will also get
		// appended to the actionDescrion to return to the user.
		allStatements, err := conn.GenerateApplyStatements(ctx, handle, materializationSpec)
		if err != nil {
			return nil, err
		}

		if !req.DryRun {
			logger.Infof("Executing DDL to apply materialization")
			err = conn.ExecApplyStatements(ctx, handle, allStatements)
			if err != nil {
				return nil, err
			}
		}
		// Like my grandpappy always told me, "never generate a SQL file without a comment at the top"
		var comment = g.SQLGen.Comment(fmt.Sprintf(
			"Generated by Flow for materializing collection '%s'\nto table: %s",
			req.Collection.Collection,
			handle.Table,
		))
		// We'll wrap this in BEGIN and COMMIT just to try to be helpful and mimic the transaction we
		// run here.
		response.ActionDescription = fmt.Sprintf("%s\nBEGIN;\n%s\nCOMMIT;\n",
			comment, strings.Join(allStatements, "\n\n"))

	} else {
		logger.Debug("Skipping execution of SQL because materialization has already been applied")
	}

	return response, nil
}

func (g *GenericDriver) getCachedSQL(ctx context.Context, handle *Handle, conn Connection) (*CachedSQL, error) {
	// We could alternatively use a concurrent map and a separate mutex per CallerId, but I decided
	// to KISS for now since it's doubtful that the simple single mutex will actually cause a
	// problem.
	g.sqlCacheMutex.Lock()
	defer g.sqlCacheMutex.Unlock()
	var cachedSQL = g.SQLCache[handle.ShardID]
	// Do we need to re-create the sql queries?
	if cachedSQL == nil || cachedSQL.nonce != handle.Nonce {
		var spec, err = conn.QueryMaterializationSpec(ctx, handle)
		if err != nil {
			return nil, fmt.Errorf("Failed to query materialization spec for '%s': %w", handle.Table, err)
		}

		newSQL, err := conn.GenerateRuntimeSQL(ctx, handle, spec)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate sql statements for '%s': %w", handle.ShardID, err)
		}
		log.WithFields(log.Fields{
			"shardId": handle.ShardID,
			"nonce":   handle.Nonce,
		}).Debugf("Generated new sql statements: %+v", newSQL)
		g.SQLCache[handle.ShardID] = newSQL
		cachedSQL = newSQL
	}
	return cachedSQL, nil
}

func (g *GenericDriver) doValidate(ctx context.Context, handle *Handle, proposed *pf.CollectionSpec, currentSpec *MaterializationSpec) (map[string]*pm.Constraint, error) {
	var err = proposed.Validate()
	if err != nil {
		return nil, fmt.Errorf("The proposed CollectionSpec is invalid: %w", err)
	}

	var constraints map[string]*pm.Constraint
	if currentSpec != nil {
		// Ensure that the existing spec is valid, since it may have been modified manually.
		if err = currentSpec.Validate(); err != nil {
			return nil, fmt.Errorf("The existing MaterializationSpec is invalid: %w", err)
		}
		constraints = ValidateMatchesExisting(currentSpec, proposed)
	} else {
		constraints = ValidateNewSQLProjections(proposed)
	}
	return constraints, nil
}
