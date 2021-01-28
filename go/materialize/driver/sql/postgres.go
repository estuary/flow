package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/estuary/flow/go/fdb/tuple"
	//pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

// EndpointTypePostgres is the name of the endpoint type for postgresql, used in the catalog spec.
const JoinQuerySql = "joinQuery"

var PgTxnOptions = pgx.TxOptions{
	IsoLevel: pgx.RepeatableRead,
}

// NewPostgresDriver returns a new driver for materializations into postgres.
func NewPostgresDriver() pm.DriverServer {
	var sqlGen = PostgresSQLGenerator()
	var connectionBuilder = pgConnectionManager{sqlGen: &sqlGen}
	return &GenericDriver{
		SQLGen:      &sqlGen,
		Connections: NewCache(&connectionBuilder),
		SQLCache:    make(map[string]*CachedSQL),
	}
}

type pgTxnImpl struct {
	ctx                context.Context
	conn               pgxpool.Conn
	txn                pgx.Tx
	logEntry           *log.Entry
	loadKeyCh          <-chan tuple.Tuple
	loadQuery          string
	loadedDocumentCh   chan<- LoadedDocument
	keyParamsConverter ParametersConverter

	storeDocumentCh       <-chan StoreDocument
	insertStatement       string
	insertParamsConverter ParametersConverter
	updateStatement       string
	updateParamsConverter ParametersConverter
	commitCh              chan<- error

	keyColumns []string
}

type pgKeyLoader struct {
	nKeys   int
	nextRow []interface{}
	err     error
	txn     *pgTxnImpl
}

// Next is part of the pgx.CopyFromSource implementation
func (t *pgKeyLoader) Next() bool {
	// Allow multiple calls to Next to be idempotent
	if t.nextRow != nil {
		return true
	}

	select {
	case loaded, ok := <-t.txn.loadKeyCh:
		if ok {
			var nextRow, err = t.txn.keyParamsConverter.ConvertTuple(loaded)
			if err != nil {
				t.err = fmt.Errorf("failed to convert key tuple: %w", err)
				return false
			} else {
				t.nextRow = nextRow
				return true
			}
		} else {
			return false
		}
	case <-t.txn.ctx.Done():
		return false
	}
}

// Values is part of the pgx.CopyFromSource implementation
func (t *pgKeyLoader) Values() ([]interface{}, error) {
	if t.err != nil {
		return nil, t.err
	} else {
		t.nKeys++
		var key = t.nextRow
		// Set to nil so that a subsequent call to Next will repopulate it
		t.nextRow = nil
		return key, nil
	}
}

// Err is part of the pgx.CopyFromSource implementation
func (t *pgKeyLoader) Err() error {
	return t.err
}

func (t *pgTxnImpl) rollback(err error) {
	// TODO: should we always send the error on both channels?
	if t.loadedDocumentCh != nil {
		select {
		case t.loadedDocumentCh <- LoadedDocument{Error: err}:
		case <-t.ctx.Done():
		}
		close(t.loadedDocumentCh)
	}
	var rbErr = t.txn.Rollback(t.ctx)
	t.logEntry.WithField("error", err).Warnf("rolled back transaction with result: %v", rbErr)
	select {
	case t.commitCh <- err:
	case <-t.ctx.Done():
	}
	close(t.commitCh)
}

func (t *pgTxnImpl) runTransaction() (err error) {
	t.logEntry.Trace("Starting to read loadKeyCh")
	var committed = false
	defer func() {
		if err != nil && !committed {
			t.rollback(err)
		}
		t.conn.Release()
	}()

	var keyLoader = pgKeyLoader{
		txn: t,
	}

	// Does the client wish to load at least one document?
	// I'm uncertain whether postgres allows a CopyFrom without any values, but early indications
	// seem to be negative, and we might as well skip the extra round trips anyway.
	if keyLoader.Next() {
		t.logEntry.Debug("at least one key was provided, copying keys into temp table")
		nkeys, err := t.txn.CopyFrom(t.ctx, pgx.Identifier{TempKeyTableName}, t.keyColumns, &keyLoader)
		if err != nil {
			return fmt.Errorf("failed to copy keys into temp table: %w", err)
		}
		t.logEntry.WithField("nrows", nkeys).Debug("finished loading keys")

		rows, err := t.txn.Query(t.ctx, t.loadQuery)
		if err != nil {
			return fmt.Errorf("failed to query documents: %w", err)
		}
		var foundDocs = 0
		for rows.Next() {
			foundDocs++
			var json json.RawMessage
			err = rows.Scan(&json)
			if err != nil {
				return err
			}
			select {
			case t.loadedDocumentCh <- LoadedDocument{Document: json}:
			case <-t.ctx.Done():
				return fmt.Errorf("context canceled")
			}
		}
		t.logEntry.WithFields(log.Fields{
			"nKeys":   nkeys,
			"nLoaded": foundDocs,
		}).Debug("returned loaded documents")
	} else {
		t.logEntry.Debug("No keys to load")
	}

	close(t.loadedDocumentCh)
	t.loadedDocumentCh = nil

	var batch = &pgx.Batch{}
	for storeDoc := range t.storeDocumentCh {
		if committed {
			panic("expected Commit to be the final message int he transaction")
		}
		if storeDoc.Commit {
			// How many insert/update statements did we execute in this batch?
			var nResults = batch.Len()
			t.logEntry.WithField("nRows", nResults).Debug("Starting to execute batch")
			var batchResults = t.txn.SendBatch(t.ctx, batch)
			for i := 0; i < nResults; i++ {
				_, err = batchResults.Exec()
				if err != nil {
					return fmt.Errorf("Failed to store document at index: %d: %w", i, err)
				}
			}
			err = batchResults.Close()
			if err != nil {
				return fmt.Errorf("failed to close batch: %w", err)
			}
			t.logEntry.Debug("Starting transaction commit")
			committed = true
			err = t.txn.Commit(t.ctx)
			if err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
			t.logEntry.Debug("Transaction committed successfully")
			select {
			case t.commitCh <- nil:
			case <-t.ctx.Done():
			}
		} else if storeDoc.Update {
			var updateParams []interface{}
			updateParams = append(updateParams, storeDoc.Values.ToInterface()...)
			updateParams = append(updateParams, storeDoc.Document)
			updateParams = append(updateParams, storeDoc.Key.ToInterface()...)
			updateParams, err = t.updateParamsConverter.Convert(updateParams...)
			if err != nil {
				return fmt.Errorf("failed to convert update parameters")
			}
			batch.Queue(t.updateStatement, updateParams...)
		} else {
			var insertParams []interface{}
			insertParams = append(insertParams, storeDoc.Key.ToInterface()...)
			insertParams = append(insertParams, storeDoc.Values.ToInterface()...)
			insertParams = append(insertParams, storeDoc.Document)
			insertParams, err = t.insertParamsConverter.Convert(insertParams...)
			if err != nil {
				return fmt.Errorf("failed to convert insert parameters")
			}
			batch.Queue(t.insertStatement, insertParams...)
		}
	}
	return nil
}

type pgConnection struct {
	pool   *pgxpool.Pool
	sqlGen SQLGenerator
}

const TempKeyTableName = "flow_load_key_tmp"

func loadKeyTempTable(spec *MaterializationSpec) *Table {
	var columns = make([]Column, len(spec.Fields.Keys))
	for i, keyField := range spec.Fields.Keys {
		var projection = spec.Collection.GetProjection(keyField)
		columns[i] = columnForProjection(projection)
	}
	return &Table{
		Name:         TempKeyTableName,
		Columns:      columns,
		IfNotExists:  true,
		Temporary:    true,
		TempOnCommit: "DELETE ROWS",
	}
}

// StartTransaction implements the Connection interface
func (c *pgConnection) StartTransaction(ctx context.Context, handle *Handle, flowCheckpoint []byte, cachedSQL *CachedSQL) (_ Transaction, retErr error) {
	var logEntry = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
	})
	var txn, err = c.pool.BeginTx(ctx, PgTxnOptions)
	if err != nil {
		return Transaction{}, err
	}

	defer func() {
		if retErr != nil {
			var rbErr = txn.Rollback(ctx)
			logEntry.WithField("error", retErr).Warnf("failed to start transaction, rolled back with result: %v", rbErr)
		}
	}()

	var updateCheckpoint = cachedSQL.statements[UpdateCheckpointKey]
	var cpConverter = cachedSQL.parameterConverters[UpdateCheckpointKey]
	updateCPArgs, err := cpConverter.Convert(flowCheckpoint, handle.ShardID)
	if err != nil {
		return Transaction{}, err
	}
	cpUpdateResult, err := txn.Exec(ctx, updateCheckpoint, updateCPArgs...)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to update flow checkpoint: %w", err)
	}
	var affectedRows = cpUpdateResult.RowsAffected()
	if affectedRows != 1 {
		return Transaction{}, fmt.Errorf("update of flow checkpoint affected %d rows, expected 1", affectedRows)
	}
	logEntry.Debug("successfully updated flow checkpoint")

	// TODO: move tempTable sql onto cachedSQL
	var tempTable = loadKeyTempTable(cachedSQL.spec)
	createTableSql, err := c.sqlGen.CreateTable(tempTable)
	if err != nil {
		return Transaction{}, err
	}
	_, err = txn.Exec(ctx, createTableSql)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to create temp table: %w", err)
	}

	var query = strings.Join([]string{
		"SELECT",
		cachedSQL.spec.Fields.Document,
		"FROM",
		TempKeyTableName,
		"NATURAL JOIN",
		handle.Table,
	}, " ")

	var loadKeyCh = make(chan tuple.Tuple)
	var loadedDocumentCh = make(chan LoadedDocument)
	var storeDocumentCh = make(chan StoreDocument)
	var commitCh = make(chan error)

	var impl = pgTxnImpl{
		ctx:              ctx,
		txn:              txn,
		logEntry:         logEntry,
		loadKeyCh:        loadKeyCh,
		loadQuery:        query,
		loadedDocumentCh: loadedDocumentCh,
		storeDocumentCh:  storeDocumentCh,
		commitCh:         commitCh,

		keyColumns:            cachedSQL.spec.Fields.Keys,
		keyParamsConverter:    cachedSQL.parameterConverters[LoadQueryKey],
		updateStatement:       cachedSQL.statements[UpdateDocumentKey],
		updateParamsConverter: cachedSQL.parameterConverters[UpdateDocumentKey],
		insertParamsConverter: cachedSQL.parameterConverters[InsertDocumentKey],
		insertStatement:       cachedSQL.statements[InsertDocumentKey],
	}

	go impl.runTransaction()

	var transaction = Transaction{
		LoadKeyCh:        loadKeyCh,
		LoadedDocumentCh: loadedDocumentCh,
		StoreDocumentCh:  storeDocumentCh,
		CommitCh:         commitCh,
	}
	return transaction, nil
}

// QueryMaterializationSpec implements the Connection interface
func (c *pgConnection) QueryMaterializationSpec(ctx context.Context, handle *Handle) (*MaterializationSpec, error) {
	var table = FlowMaterializationsTable()
	query, paramConverter, err := c.sqlGen.QueryOnPrimaryKey(table, FlowMaterializationsSpecColumn)
	if err != nil {
		return nil, err
	}
	convertedKey, err := paramConverter.Convert(handle.Table)
	if err != nil {
		return nil, err
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	var row = conn.QueryRow(ctx, query, convertedKey...)

	var jsonStr string
	err = row.Scan(&jsonStr)
	if err == pgx.ErrNoRows {
		return nil, nil
	} else if err != nil {
		log.WithFields(log.Fields{
			"shardId": handle.ShardID,
			"nonce":   handle.Nonce,
			"error":   err,
		}).Debugf("failed to query materializationSpec. This is possibly due to the table not being initialized")
		// TODO: check if flow_materializations table exists
		return nil, nil
	}

	var materializationSpec = new(MaterializationSpec)
	err = json.Unmarshal([]byte(jsonStr), materializationSpec)
	return materializationSpec, err
}

// Fence implements the Connection interface
func (c *pgConnection) Fence(ctx context.Context, handle *Handle) ([]byte, error) {
	var logger = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
	})

	var txn, err = c.pool.BeginTx(ctx, PgTxnOptions)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if txn != nil {
				var rbErr = txn.Rollback(ctx)
				logger.WithField("error", err).Errorf("Fence transaction failed, rolled back with result: %v", rbErr)
			} else {
				logger.WithField("error", err).Errorf("Fence transaction failed to commit")
			}
		}
	}()
	var gazCheckpointsTable = GazetteCheckpointsTable()
	query, queryConverter, err := c.sqlGen.QueryOnPrimaryKey(gazCheckpointsTable, GazetteCheckpointsNonceColumn, GazetteCheckpointsCheckpointColumn)
	if err != nil {
		return nil, err
	}

	queryArgs, err := queryConverter.Convert(handle.ShardID)
	if err != nil {
		return nil, err
	}

	var oldNonce int32
	var flowCheckpoint []byte
	logger.WithField("query", query).Debug("querying existing checkpoint")
	var row = txn.QueryRow(ctx, query, queryArgs...)
	err = row.Scan(&oldNonce, &flowCheckpoint)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("failed to query current flow checkpoint: %w", err)
	} else if err == pgx.ErrNoRows {
		// There's no current checkpoint value, so we'll initialize a new one
		logger.Infof("Initializing new flow checkpoint")
		var insertStmt, insertConverter, err = c.sqlGen.InsertStatement(gazCheckpointsTable)
		if err != nil {
			return nil, err
		}
		insertArgs, err := insertConverter.Convert(handle.ShardID, handle.Nonce, make([]byte, 0))
		if err != nil {
			return nil, err
		}

		// The initial value for the checkpoint is just an empty slice. The nonce will be initialized to
		// the current nonce, though.
		_, err = txn.Exec(ctx, insertStmt, insertArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize flow checkpoint: %w", err)
		}
	} else {
		// There's already a checkpoint present
		var whereColumns = []string{GazetteCheckpointsShardIDColumn, GazetteCheckpointsNonceColumn}
		var setColumns = []string{GazetteCheckpointsNonceColumn}
		var updateSQL, updateConverter, err = c.sqlGen.UpdateStatement(gazCheckpointsTable, setColumns, whereColumns)
		if err != nil {
			return nil, err
		}
		updateArgs, err := updateConverter.Convert(handle.Nonce, handle.ShardID, oldNonce)
		_, err = txn.Exec(ctx, updateSQL, updateArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to update nonce for flow checkpoint: %w", err)
		}
	}
	err = txn.Commit(ctx)
	txn = nil // set to nil so we don't try to rollback if commit fails
	if err != nil {
		return nil, err
	}
	return flowCheckpoint, err
}

// GenerateRuntimeSQL implements the Connection interface
func (c *pgConnection) GenerateRuntimeSQL(ctx context.Context, handle *Handle, spec *MaterializationSpec) (*CachedSQL, error) {
	return doGenerateRuntimeSql(handle, spec, c.sqlGen)
}

// GenerateApplyStatements implements the Connection interface
func (c *pgConnection) GenerateApplyStatements(ctx context.Context, handle *Handle, spec *MaterializationSpec) ([]string, error) {
	return doGenerateApplyStatements(handle.Table, c.sqlGen, spec)
}

// ExecApplyStatements implements the Connection interface
func (c *pgConnection) ExecApplyStatements(ctx context.Context, handle *Handle, statements []string) error {
	var txn, err = c.pool.Begin(ctx)
	if err != nil {
		return err
	}

	for i, stmt := range statements {
		_, err = txn.Exec(ctx, stmt)
		if err != nil {
			var rbErr = txn.Rollback(ctx)
			log.WithFields(log.Fields{
				"nonce":   handle.Nonce,
				"shardId": handle.ShardID,
				"error":   err,
			}).Warnf("Failed to exec apply statements, rolled back transaction with result: %v", rbErr)
			return fmt.Errorf("failed to execute sql statement %d of %d: %w", i+1, len(statements), err)
		}
	}
	return txn.Commit(ctx)
}

type pgConnectionManager struct {
	sqlGen SQLGenerator
}

func (m *pgConnectionManager) Connection(ctx context.Context, uri string) (Connection, error) {
	var pool, err = pgxpool.Connect(ctx, uri)
	if err != nil {
		return nil, err
	}
	return &pgConnection{
		pool:   pool,
		sqlGen: m.sqlGen,
	}, nil
}
