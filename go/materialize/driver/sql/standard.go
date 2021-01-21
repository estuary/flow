package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"

	// Below are imports needed by the go sql package. These are not used directly, but they are
	// required in order to connect to the databases.
	// The sqlite driver
	_ "github.com/mattn/go-sqlite3"
	// The postgresql driver
	_ "github.com/lib/pq"
)

const (
	// EndpointTypePostgres is the name of the endpoint type for postgresql, used in the catalog spec.
	EndpointTypePostgres = "postgres"
	// EndpointTypeSQLite is the name of the endpoint type for sqlite, used in the catalog spec.
	EndpointTypeSQLite = "sqlite"

	// GazetteCheckpointsShardIDColumn is the name of the column that holds the shard id in the gazette_checkpoints table.
	GazetteCheckpointsShardIDColumn = "shard_id"
	// GazetteCheckpointsNonceColumn is the name of the column that holds the nonce in the gazette_checkpoints table.
	GazetteCheckpointsNonceColumn = "nonce"
	// GazetteCheckpointsCheckpointColumn is the name of the column that holds the checkpoint in the gazette_checkpoints table.
	GazetteCheckpointsCheckpointColumn = "checkpoint"

	// FlowMaterializationsSpecColumn is the name of the column that holds the materialization spec in the flow_materializations
	// table.
	FlowMaterializationsSpecColumn = "spec"
)

// GazetteCheckpointsTable returns the Table description for the table that holds the checkpoint
// and nonce values for each materialization shard.
func GazetteCheckpointsTable() *Table {
	return &Table{
		Name:        "gazette_checkpoints",
		IfNotExists: true,
		Comment:     "This table holds journal checkpoints, which Flow manages in order to ensure exactly-once updates for materializations",
		Columns: []Column{
			{
				Name:       GazetteCheckpointsShardIDColumn,
				Comment:    "The id of the consumer shard. Note that a single collection may have multiple consumer shards materializing it, and each will have a separate checkpoint.",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:    GazetteCheckpointsNonceColumn,
				Comment: "This nonce is used to uniquely identify unique process assignments of a shard and prevent them from conflicting.",
				Type:    INTEGER,
				NotNull: true,
			},
			{
				Name:    GazetteCheckpointsCheckpointColumn,
				Comment: "Opaque checkpoint of the Flow consumer shard",
				Type:    BINARY,
			},
		},
	}
}

// FlowMaterializationsTable returns the Table description for the table that holds the
// MaterializationSpec that corresponds to each target table. This state is used both for sql
// generation and for validation.
func FlowMaterializationsTable() *Table {
	return &Table{
		Name:        "flow_materializations",
		IfNotExists: true,
		Comment:     "This table is the source of truth for all materializations into this system.",
		Columns: []Column{
			{
				Name:       "table_name",
				Comment:    "The name of the target table of the materialization, which may or may not include a schema and catalog prefix",
				PrimaryKey: true,
				Type:       STRING,
				NotNull:    true,
			},
			{
				Name:    FlowMaterializationsSpecColumn,
				Comment: "A JSON representation of the materialization.",
				Type:    OBJECT,
				NotNull: true,
			},
		},
	}
}

// ArenaAppender allows copying bytes from the database directly into the response Arena by
// implementing the sql.Scanner interface.
type ArenaAppender struct {
	arena  *pf.Arena
	slices []pf.Slice
}

// Scan implements sql.Scanner for ArenaAppender
func (a *ArenaAppender) Scan(src interface{}) error {
	var slice pf.Slice
	switch ty := src.(type) {
	case []byte:
		slice = a.arena.Add(ty)
	case string:
		slice = a.arena.Add([]byte(ty))
	default:
		return fmt.Errorf("arenaAppender can only scan []byte and string values")
	}
	if slice.Begin != slice.End {
		a.slices = append(a.slices, slice)
	}
	return nil
}

type StandardSQLTransaction struct {
	txn             *sql.Tx
	updateStatement *sql.Stmt
	insertStatement *sql.Stmt
	queryStatement  *sql.Stmt
	loadKeys        [][]interface{}
}

func (t *StandardSQLTransaction) AddLoadKey(ctx context.Context, key []interface{}) error {
	// To keep things simple and generic, we'll just execute a prepared statement for each key. This
	// will be much less efficient than batching, but it's easy to implement on top of a "lowest
	// common denominator" interface.
	t.loadKeys = append(t.loadKeys, key)
	return nil
}

func (t *StandardSQLTransaction) PollLoadResults(ctx context.Context, arena *pf.Arena) ([]pf.Slice, error) {
	var appender = ArenaAppender{
		arena: arena,
	}

	for _, key := range t.loadKeys {
		var row = t.queryStatement.QueryRowContext(ctx, key...)
		var err = row.Scan(&appender)
		if err != nil && err != sql.ErrNoRows {
			return nil, fmt.Errorf("failed to scan results: %w", err)
		}
	}
	t.loadKeys = t.loadKeys[:0]
	return appender.slices, nil
}

func (t *StandardSQLTransaction) FlushLoadResults(ctx context.Context, arena *pf.Arena) ([]pf.Slice, error) {
	return t.PollLoadResults(ctx, arena)
}

func (t *StandardSQLTransaction) Insert(ctx context.Context, args []interface{}) error {
	var _, err = t.insertStatement.ExecContext(ctx, args...)
	return err
}

func (t *StandardSQLTransaction) Update(ctx context.Context, args []interface{}) error {
	var _, err = t.updateStatement.ExecContext(ctx, args...)
	return err
}

func (t *StandardSQLTransaction) Commit(ctx context.Context) error {
	return t.txn.Commit()
}

func (t *StandardSQLTransaction) Rollback() error {
	return t.txn.Rollback()
}

type StandardSQLConnection struct {
	TxnOpts *sql.TxOptions
	DB      *sql.DB
	SQLGen  SQLGenerator
}

// StarStartTransaction implements Conn
func (c *StandardSQLConnection) StartTransaction(ctx context.Context, handle *Handle, flowCheckpoint []byte, cachedSQL *CachedSQL) (Transaction, error) {
	var txn, err = c.DB.BeginTx(ctx, c.TxnOpts)
	if err != nil {
		return nil, err
	}

	updateCheckpoint, cpConverter, err := c.SQLGen.UpdateStatement(
		GazetteCheckpointsTable(),
		[]string{GazetteCheckpointsCheckpointColumn},
		[]string{GazetteCheckpointsShardIDColumn},
	)
	if err != nil {
		return nil, err
	}
	cpUpdate, err := txn.PrepareContext(ctx, updateCheckpoint)
	if err != nil {
		return nil, err
	}
	args, err := cpConverter.Convert(flowCheckpoint, handle.ShardID)
	if err != nil {
		return nil, err
	}
	result, err := cpUpdate.ExecContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get number of affected rows for checkpoint update: %w", err)
	}
	if nRows != 1 {
		return nil, fmt.Errorf("Expected 1 flow checkpoint updated, but was %d", nRows)
	}

	insertStatement, err := txn.PrepareContext(ctx, cachedSQL.insertStatement)
	if err != nil {
		return nil, fmt.Errorf("insert statement error: %w", err)
	}
	updateStatement, err := txn.PrepareContext(ctx, cachedSQL.updateStatement)
	if err != nil {
		return nil, fmt.Errorf("update statement error: %w", err)
	}
	queryStatement, err := txn.PrepareContext(ctx, cachedSQL.loadQuery)
	if err != nil {
		return nil, fmt.Errorf("query statement error: %w", err)
	}
	return &StandardSQLTransaction{
		txn:             txn,
		insertStatement: insertStatement,
		updateStatement: updateStatement,
		queryStatement:  queryStatement,
	}, nil
}

func (c *StandardSQLConnection) GenerateApplyStatements(ctx context.Context, handle *Handle, spec *MaterializationSpec) ([]string, error) {
	var gazetteCheckpointsTable = GazetteCheckpointsTable()
	var flowMaterializationsTable = FlowMaterializationsTable()

	// Like my grandpappy always told me, "never generate a SQL file without a comment at the top"
	var comment = c.SQLGen.Comment(fmt.Sprintf(
		"Generated by Flow for materializing collection '%s'\nto table: %s",
		spec.Collection.Collection,
		handle.Table,
	))
	var userTable = tableForMaterialization(handle.Table, comment, spec)

	createFlowCheckpointsTable, err := c.SQLGen.CreateTable(gazetteCheckpointsTable)
	if err != nil {
		return nil, err
	}
	createFlowMaterializationsTable, err := c.SQLGen.CreateTable(flowMaterializationsTable)
	if err != nil {
		return nil, err
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal materialization spec: %w", err)
	}

	insertMaterializationSpec, err := c.SQLGen.DirectInsertStatement(flowMaterializationsTable, handle.Table, string(specJSON))
	if err != nil {
		return nil, err
	}

	createTargetTable, err := c.SQLGen.CreateTable(userTable)
	if err != nil {
		return nil, err
	}

	return []string{
		createFlowCheckpointsTable,
		createFlowMaterializationsTable,
		insertMaterializationSpec,
		createTargetTable,
	}, nil
}

func (c *StandardSQLConnection) ExecApplyStatements(ctx context.Context, handle *Handle, statements []string) (retErr error) {
	var txn, err = c.DB.BeginTx(ctx, c.TxnOpts)
	if err != nil {
		return err
	}

	var logger = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
		"table":   handle.Table,
	})
	logger.Debug("Starting to execute Apply statements")

	for i, stmt := range statements {
		_, err := txn.ExecContext(ctx, stmt)
		if err != nil {
			var rbErr = txn.Rollback()
			logger.WithField("error", err).Warnf("failed to execute Apply statement %d, rolled back transaction with result: %v", i, rbErr)
			return fmt.Errorf("Failed to execute statement %d: %w", i, err)
		}
	}
	return txn.Commit()
}

func (c *StandardSQLConnection) QueryMaterializationSpec(ctx context.Context, handle *Handle) (*MaterializationSpec, error) {
	// TODO: maybe add a function to check if a table exists, so we can have better error handling
	// We call PingContext in an attempt to differentiate between the table being missing vs
	// some other more serious error. If Ping returns nil, then we'll assume that the original
	// error was simply due to the flow_materializations table being missing.
	var err = c.DB.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	var table = FlowMaterializationsTable()
	query, paramConverter, err := c.SQLGen.QueryOnPrimaryKey(table, FlowMaterializationsSpecColumn)
	if err != nil {
		return nil, err
	}
	convertedKey, err := paramConverter.Convert(handle.Table)
	if err != nil {
		return nil, err
	}

	var row = c.DB.QueryRowContext(ctx, query, convertedKey...)

	var jsonStr string
	err = row.Scan(&jsonStr)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		log.WithFields(log.Fields{
			"shardId": handle.ShardID,
			"nonce":   handle.Nonce,
			"error":   err,
		}).Debugf("failed to query materializationSpec. This is possibly due to the table not being initialized")
		return nil, nil
	}

	var materializationSpec = new(MaterializationSpec)
	err = json.Unmarshal([]byte(jsonStr), materializationSpec)
	return materializationSpec, err
}

func (c *StandardSQLConnection) Fence(ctx context.Context, handle *Handle) ([]byte, error) {
	var logger = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
	})

	txn, err := c.DB.BeginTx(ctx, c.TxnOpts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if txn != nil {
				var rbErr = txn.Rollback()
				logger.WithField("error", err).Errorf("Fence transaction failed, rolled back with result: %v", rbErr)
			} else {
				logger.WithField("error", err).Errorf("Fence transaction failed to commit")
			}
		}
	}()
	var gazCheckpointsTable = GazetteCheckpointsTable()
	query, queryConverter, err := c.SQLGen.QueryOnPrimaryKey(gazCheckpointsTable, GazetteCheckpointsNonceColumn, GazetteCheckpointsCheckpointColumn)
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
	var row = txn.QueryRowContext(ctx, query, queryArgs...)
	err = row.Scan(&oldNonce, &flowCheckpoint)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to query current flow checkpoint: %w", err)
	} else if err == sql.ErrNoRows {
		// There's no current checkpoint value, so we'll initialize a new one
		logger.Infof("Initializing new flow checkpoint")
		var insertStmt, insertConverter, err = c.SQLGen.InsertStatement(gazCheckpointsTable)
		if err != nil {
			return nil, err
		}
		insertArgs, err := insertConverter.Convert(handle.ShardID, handle.Nonce, make([]byte, 0))
		if err != nil {
			return nil, err
		}

		// The initial value for the checkpoint is just an empty slice. The nonce will be initialized to
		// the current nonce, though.
		_, err = txn.ExecContext(ctx, insertStmt, insertArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize flow checkpoint: %w", err)
		}
	} else {
		// There's already a checkpoint present
		var whereColumns = []string{GazetteCheckpointsShardIDColumn, GazetteCheckpointsNonceColumn}
		var setColumns = []string{GazetteCheckpointsNonceColumn}
		var updateSQL, updateConverter, err = c.SQLGen.UpdateStatement(gazCheckpointsTable, setColumns, whereColumns)
		if err != nil {
			return nil, err
		}
		updateArgs, err := updateConverter.Convert(handle.Nonce, handle.ShardID, oldNonce)
		_, err = txn.ExecContext(ctx, updateSQL, updateArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to update nonce for flow checkpoint: %w", err)
		}
	}
	err = txn.Commit()
	txn = nil // set to nil so we don't try to rollback if commit fails
	if err != nil {
		return nil, err
	}
	return flowCheckpoint, err
}

type StandardSQLConnectionManager struct {
	DriverName string
	SQLGen     SQLGenerator
	TxOptions  sql.TxOptions
}

func (m *StandardSQLConnectionManager) Connection(ctx context.Context, handle *Handle) (Connection, error) {
	log.WithFields(log.Fields{
		"shardId":   handle.ShardID,
		"nonce":     handle.Nonce,
		"sqlDriver": m.DriverName,
	}).Info("opening new database connection pool")
	var conn, err = sql.Open(m.DriverName, handle.URI)
	if err != nil {
		return nil, err
	}
	return &StandardSQLConnection{
		TxnOpts: &m.TxOptions,
		DB:      conn,
		SQLGen:  m.SQLGen,
	}, nil
}

var _ ConnectionManager = (*StandardSQLConnectionManager)(nil)

// Handle is the parsed representation of what we return from a StartSession rpc.
type Handle struct {
	// Nonce is a unique number that is randomly generated every time a session is started.
	Nonce int32 `json:"nonce"`
	// URI is the connection string for the target system.
	URI string `json:"uri"`
	// Table is the name of the table that we'll be materializing into.
	Table string `json:"table"`
	// CallerID represents the stable id of the shard or process that this session belongs to.
	ShardID string `json:"callerId"`
}

// CachedSQL holds all of the sql statements that we cache.
type CachedSQL struct {
	nonce                 int32
	loadQuery             string
	QueryKeyConverter     ParametersConverter
	insertStatement       string
	InsertValuesConverter ParametersConverter
	updateStatement       string
	UpdateValuesConverter ParametersConverter
	primaryKeys           []bool
}

// GenerGenerateRuntimeSQL implements ConnectionWrapper for StandardSQLConnection
func (c *StandardSQLConnection) GenerateRuntimeSQL(ctx context.Context, handle *Handle, spec *MaterializationSpec) (*CachedSQL, error) {
	var targetTable = tableForMaterialization(handle.Table, "", spec)

	loadQuery, loadConverter, err := c.SQLGen.QueryOnPrimaryKey(targetTable, spec.Fields.Document)
	if err != nil {
		return nil, err
	}
	insertStatement, insertConverter, err := c.SQLGen.InsertStatement(targetTable)
	if err != nil {
		return nil, err
	}

	var setColumns []string
	setColumns = append(setColumns, spec.Fields.Values...)
	setColumns = append(setColumns, spec.Fields.Document)

	updateStatement, updateConverter, err := c.SQLGen.UpdateStatement(targetTable, setColumns, spec.Fields.Keys)
	if err != nil {
		return nil, err
	}

	return &CachedSQL{
		nonce:             handle.Nonce,
		loadQuery:         loadQuery,
		QueryKeyConverter: loadConverter,

		insertStatement:       insertStatement,
		InsertValuesConverter: insertConverter,

		updateStatement:       updateStatement,
		UpdateValuesConverter: updateConverter,
	}, nil
}

func parseHandle(bytes []byte) (*Handle, error) {
	var handle = new(Handle)
	var err = json.Unmarshal(bytes, handle)
	return handle, err
}

// tableForMaterialization converts a MaterializationSpec into the Table representation that's used
// by the SQLGenerator. This assumes that the MaterializationSpec has already been validated to
// ensure that each projection has exactly one type besides "null".
func tableForMaterialization(name string, comment string, spec *MaterializationSpec) *Table {
	return &Table{
		Name:    name,
		Comment: comment,
		Columns: columnsForMaterialization(spec),
	}
}

// Returns a slice of Columns for the materialization. This function always puts the root document
// projection at the end, so it's always at a known position for dealing with insert and update
// statements.
func columnsForMaterialization(spec *MaterializationSpec) []Column {
	var allFields = spec.Fields.AllFields()
	var columns = make([]Column, 0, len(allFields))
	for _, field := range allFields {
		var projection = spec.Collection.GetProjection(field)
		columns = append(columns, columnForProjection(projection))
	}
	return columns
}

func columnForProjection(projection *pf.Projection) Column {
	var column = Column{
		Name:       projection.Field,
		Comment:    commentForProjection(projection),
		PrimaryKey: projection.IsPrimaryKey,
		Type:       columnType(projection),
		NotNull:    projection.Inference.MustExist && !sliceContains("null", projection.Inference.Types),
	}
	if projection.Inference.String_ != nil {
		var s = projection.Inference.String_
		column.StringType = &StringTypeInfo{
			Format:      s.Format,
			ContentType: s.ContentType,
			MaxLength:   s.MaxLength,
		}
	}
	return column
}

func columnType(projection *pf.Projection) ColumnType {
	for _, ty := range projection.Inference.Types {
		switch ty {
		case "string":
			return STRING
		case "integer":
			return INTEGER
		case "number":
			return NUMBER
		case "boolean":
			return BOOLEAN
		case "object":
			return OBJECT
		case "array":
			return ARRAY
		}
	}
	panic("attempt to create column with no non-null type")
}

func commentForProjection(projection *pf.Projection) string {
	var source = "auto-generated"
	if projection.UserProvided {
		source = "user-provided"
	}
	var types = strings.Join(projection.Inference.Types, ", ")
	return fmt.Sprintf("%s projection of JSON at: %s with inferred types: [%s]", source, projection.Ptr, types)
}
