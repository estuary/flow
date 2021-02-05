package driver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"reflect"
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

// cachedSQL holds all of the sql statements that we cache.
type cachedSQL struct {
	nonce           int32
	loadQuery       string
	insertStatement string
	updateStatement string
	primaryKeys     []bool
}

// A SQLDriver implements the Driver service of the materialize grpc protocol using the generic
// `database/sql` package. It uses a SQLGenerator to generate sql statements that are executed by
// the `database/sql` package.
type SQLDriver struct {
	// EndpointType is the type of the endpoint from the flow.yaml spec
	EndpointType string
	// SQLDriverType is the driver name to use when creating sql connections
	SQLDriverType string
	// The SQLGenerator to use for generating statements for use with the go sql package.
	SQLGen SQLGenerator

	// map of connection URI to DB for that system. We don't ever remove anythign from this map, so
	// it'll just keep growing if someone makes a bunch of requests for many distinct endpoints.
	// A possible future optimization would be to only cache connections that are used in load/store
	// operations, but I'm holding off on that for the sake of simplicity.
	connections      map[string]*sql.DB
	connectionsMutex sync.Mutex
	// map of caller id to the sql that we cache.
	sqlCache      map[string]*cachedSQL
	sqlCacheMutex sync.Mutex
}

// NewSQLiteDriver creates a new DriverServer for sqlite.
func NewSQLiteDriver() pm.DriverServer {
	var sqlGen = SQLiteSQLGenerator()
	return &SQLDriver{
		EndpointType:  "sqlite",
		SQLDriverType: "sqlite3",
		SQLGen:        &sqlGen,
		connections:   map[string]*sql.DB{},
		sqlCache:      map[string]*cachedSQL{},
	}
}

// Assert that SQLDriver implements the DriverServer interface
var _ pm.DriverServer = &SQLDriver{}

// StartSession is part of the DriverServer implementation.
func (driver *SQLDriver) StartSession(ctx context.Context, req *pm.SessionRequest) (*pm.SessionResponse, error) {
	var handle = Handle{
		Nonce:   rand.Int31(),
		URI:     req.EndpointUrl,
		Table:   req.Target,
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
func (driver *SQLDriver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}
	// If this materialization has already been applied, then validation will ensure that the fields
	// and types of the two are the same.
	currentSpec, err := driver.loadMaterializationSpec(ctx, handle)
	if err != nil {
		return nil, err
	}

	constraints, err := driver.doValidate(ctx, handle, req.Collection, currentSpec)
	if err != nil {
		return nil, err
	}
	var response = new(pm.ValidateResponse)
	response.Constraints = constraints
	return response, nil
}

// Apply is part of the DriverServer implementation.
func (driver *SQLDriver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}

	// Has this materialization has already been applied?
	currentMaterialization, err := driver.loadMaterializationSpec(ctx, handle)
	if err != nil {
		return nil, err
	}

	// Validate the request and determine the constraints, which will then be used to validate the
	// selected fields.
	constraints, err := driver.doValidate(ctx, handle, req.Collection, currentMaterialization)
	if err != nil {
		return nil, err
	}
	// We don't handle any form of schema migrations, so we require that the list of
	// fields in the request is identical to the current fields. doValidate doesn't handle that
	// because the list of fields isn't known until Apply is called.
	if currentMaterialization != nil && !reflect.DeepEqual(req.Fields, currentMaterialization.Fields) {
		return nil, fmt.Errorf(
			"The set of fields in the request differs from the existing fields, which is disallowed because this driver does not perform schema migrations. Request fields: [%s], existing fields: [%s]",
			strings.Join(req.Fields.AllFields(), ", "),
			strings.Join(currentMaterialization.Fields.AllFields(), ", "),
		)
	}

	// If the materialization has already been applied, then we'll want to return the actionDescrion
	// with the original sql. The collectionSpec from the request could be different, even though it
	// passed validation, and so the resulting sql could be different. We always return the sql
	// corresponding to the original.
	var materializationSpec *MaterializationSpec
	if currentMaterialization != nil {
		materializationSpec = currentMaterialization
	} else {
		materializationSpec = &MaterializationSpec{
			Collection: *req.Collection,
			Fields:     *req.Fields,
		}
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
	var flowMaterializationsTable = FlowMaterializationsTable()
	var gazetteCheckpointsTable = GazetteCheckpointsTable()
	createFlowCheckpointsTable, err := driver.SQLGen.CreateTable(gazetteCheckpointsTable)
	if err != nil {
		return nil, err
	}
	createFlowMaterializationsTable, err := driver.SQLGen.CreateTable(flowMaterializationsTable)
	if err != nil {
		return nil, err
	}
	specJSON, err := json.Marshal(materializationSpec)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal materialization spec: %w", err)
	}

	insertMaterializationSpec, err := driver.SQLGen.DirectInsertStatement(flowMaterializationsTable, handle.Table, string(specJSON))
	if err != nil {
		return nil, err
	}
	var tableComment = fmt.Sprintf("Holds the materialization of the Flow collection %s", req.Collection.Name)
	var targetTable = tableForMaterialization(handle.Table, tableComment, materializationSpec)
	createTargetTable, err := driver.SQLGen.CreateTable(targetTable)
	if err != nil {
		return nil, err
	}

	// Now that we have all the sql statements, we can try executing them if desired.
	// For now, we'll always skip executing if the materialization spec already exists.
	// The semantics of Apply are a little unclear when the materialization has already been applied
	// previously. There are things we _could_ do, like trying to re-create portions of the above
	// that have been deleted. But it's not clear that there's a need for that, and the simple thing
	// is to just do nothing on the subsequent Apply calls to ensure that it's idempotent.
	if !req.DryRun && currentMaterialization == nil {
		log.WithFields(log.Fields{
			"targetTable": handle.Table,
			"shardId":     handle.ShardID,
			"collection":  req.Collection.Name,
		}).Infof("Executing DDL to apply materialization")
		var allStatements = []string{
			createFlowCheckpointsTable,
			createFlowMaterializationsTable,
			insertMaterializationSpec,
			createTargetTable,
		}
		err = driver.execTransaction(ctx, handle, allStatements)
		if err != nil {
			return nil, fmt.Errorf("failed to execute sql: %w", err)
		}
	}
	var response = new(pm.ApplyResponse)
	// Like my grandpappy always told me, "never generate a SQL file without a comment at the top"
	var comment = driver.SQLGen.Comment(fmt.Sprintf(
		"Generated by Flow for materializing collection '%s'\nto table: %s",
		req.Collection.Name,
		handle.Table,
	))
	// We'll wrap this in BEGIN and COMMIT just to try to be helpful and mimic the transaction we
	// run here.
	response.ActionDescription = fmt.Sprintf("%s\nBEGIN;\n%s\n\n%s\n\n%s\n\n%s\nCOMMIT;\n",
		comment, createFlowCheckpointsTable, createFlowMaterializationsTable,
		insertMaterializationSpec, createTargetTable)
	return response, nil
}

// Fence is part of the DriverServer implementation.
func (driver *SQLDriver) Fence(ctx context.Context, req *pm.FenceRequest) (resp *pm.FenceResponse, err error) {
	handle, err := parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}
	var logger = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
	})
	connection, err := driver.connection(handle.URI)
	if err != nil {
		return nil, err
	}
	txn, err := connection.BeginTx(ctx, &sql.TxOptions{})
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
	query, err := driver.SQLGen.QueryOnPrimaryKey(gazCheckpointsTable, GazetteCheckpointsNonceColumn, GazetteCheckpointsCheckpointColumn)
	if err != nil {
		return nil, err
	}
	queryStatement, err := txn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	var oldNonce int32
	var flowCheckpoint []byte
	var row = queryStatement.QueryRowContext(ctx, handle.ShardID)
	err = row.Scan(&oldNonce, &flowCheckpoint)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to query current flow checkpoint: %w", err)
	} else if err == sql.ErrNoRows {
		// There's no current checkpoint value, so we'll initialize a new one
		var sql, err = driver.SQLGen.InsertStatement(gazCheckpointsTable)
		if err != nil {
			return nil, err
		}
		// The initial value for the checkpoint is just an empty slice. The nonce will be initialized to
		// the current nonce, though.
		_, err = txn.ExecContext(ctx, sql, handle.ShardID, handle.Nonce, make([]byte, 0))
		if err != nil {
			return nil, fmt.Errorf("failed to initialize flow checkpoint: %w", err)
		}
	} else {
		// There's already a checkpoint present
		var whereColumns = []string{GazetteCheckpointsShardIDColumn, GazetteCheckpointsNonceColumn}
		var setColumns = []string{GazetteCheckpointsNonceColumn}
		var updateSQL, err = driver.SQLGen.UpdateStatement(gazCheckpointsTable, setColumns, whereColumns)
		if err != nil {
			return nil, err
		}
		_, err = txn.ExecContext(ctx, updateSQL, handle.Nonce, handle.ShardID, oldNonce)
		if err != nil {
			return nil, fmt.Errorf("failed to update nonce for flow checkpoint: %w", err)
		}
	}
	err = txn.Commit()
	txn = nil // set to nil so we don't try to rollback if commit fails
	if err != nil {
		return nil, err
	}
	return &pm.FenceResponse{
		FlowCheckpoint: flowCheckpoint,
	}, nil
}

// Load is part of the DriverServer implementation.
func (driver *SQLDriver) Load(ctx context.Context, req *pm.LoadRequest) (*pm.LoadResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}

	cachedSQL, err := driver.getCachedSQL(ctx, handle)
	if err != nil {
		return nil, err
	}
	connection, err := driver.connection(handle.URI)
	stmt, err := connection.PrepareContext(ctx, cachedSQL.loadQuery)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	var response = new(pm.LoadResponse)

	for _, slice := range req.PackedKeys {
		var tuple, err = tuple.Unpack(req.Arena.Bytes(slice))
		if err != nil {
			return nil, fmt.Errorf("failed to unpack key tuple: %w", err)
		}
		// Each tuple should hold the collection keys in the order provided by the Fields. This will
		// match the order in the generated sql query.
		var args = tuple.ToInterface()
		rows, err := stmt.QueryContext(ctx, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to query root document: %w", err)
		}
		// We can't defer rows.Close() since we're in a loop, so we need to be careful here
		var resultSlice pf.Slice
		var jsonBuffer sql.RawBytes
		if rows.Next() {
			err = rows.Scan(&jsonBuffer)
			if err == nil {
				resultSlice = response.Arena.Add([]byte(jsonBuffer))
			}
		} else {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read results of root document query: %w", err)
		}
		response.DocsJson = append(response.DocsJson, resultSlice)
	}
	return response, nil
}

// Store is part of the DriverServer implementation.
func (driver *SQLDriver) Store(stream pm.Driver_StoreServer) (retErr error) {
	var committed = false
	var ctx = stream.Context()
	var req, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("Failed to receive Start message: %w", err)
	}
	if req.Start == nil {
		return fmt.Errorf("Expected Start message")
	}

	handle, err := parseHandle(req.Start.Handle)
	if err != nil {
		return err
	}
	var logger = log.WithFields(log.Fields{
		"shardId": handle.ShardID,
		"nonce":   handle.Nonce,
	})

	logger.Trace("Starting Store transaction")

	cachedSQL, err := driver.getCachedSQL(ctx, handle)
	if err != nil {
		return err
	}

	// Open a transaction and prepare all the statements we'll need for processing Continue messages
	connection, err := driver.connection(handle.URI)
	if err != nil {
		return err
	}
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func(logEntry *log.Entry) {
		if retErr != nil && !committed {
			var rbErr = transaction.Rollback()
			logEntry.WithField("error", retErr).Warnf("Rolled back failed transaction with result: %v", rbErr)
		} else if retErr != nil {
			// If committed is true, then the error must have been returned from the call to commit
			logEntry.WithField("error", retErr).Warnf("Failed to commit Store transaction")
		} else {
			logEntry.Debug("Successfully committed Store transaction")
		}
	}(logger)

	insertStatement, err := transaction.PrepareContext(ctx, cachedSQL.insertStatement)
	if err != nil {
		return err
	}
	updateStatement, err := transaction.PrepareContext(ctx, cachedSQL.updateStatement)
	if err != nil {
		return err
	}

	// Update the checkpoint now. This will be part of the transaction, so it will get rolled back
	// in the event of some other error.
	updateCheckpointSQL, err := driver.SQLGen.UpdateStatement(
		GazetteCheckpointsTable(),
		[]string{GazetteCheckpointsCheckpointColumn},
		[]string{GazetteCheckpointsShardIDColumn},
	)
	if err != nil {
		return err
	}
	stmt, err := transaction.PrepareContext(ctx, updateCheckpointSQL)
	if err != nil {
		return err
	}
	result, err := stmt.ExecContext(ctx, req.Start.FlowCheckpoint, handle.ShardID)
	if err != nil {
		return err
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if nRows != 1 {
		return fmt.Errorf("Session nonce has been invalidated, checkpoint was not updated. nrows=%d", nRows)
	}

	for {
		req, err = stream.Recv()
		if err == io.EOF {
			committed = true
			logger.Debug("Starting to commit Store transaction")
			err = transaction.Commit()
			if err != nil {
				return fmt.Errorf("failed to commit store transaction: %w", err)
			}
			return stream.SendAndClose(&pm.StoreResponse{})
		} else if err != nil {
			return fmt.Errorf("failed to receive next message in stream: %w", err)
		}
		if req.Continue == nil {
			return fmt.Errorf("Expected Continue message")
		}

		for docIndex, rootDocumentSlice := range req.Continue.DocsJson {
			var packedKeysSlice = req.Continue.PackedKeys[docIndex]
			keysTuple, err := tuple.Unpack(req.Continue.Arena.Bytes(packedKeysSlice))
			if err != nil {
				return fmt.Errorf("failed to unpack tuple keys for document %d: %w", docIndex, err)
			}

			var packedValuesSlice = req.Continue.PackedValues[docIndex]
			var isUpdate = req.Continue.Exists[docIndex]
			valuesTuple, err := tuple.Unpack(req.Continue.Arena.Bytes(packedValuesSlice))
			if err != nil {
				return fmt.Errorf("failed to unpack tuple values for document %d: %w", docIndex, err)
			}

			var keyArgs = keysTuple.ToInterface()
			var valuesArgs = valuesTuple.ToInterface()
			var jsonBytes = req.Continue.Arena.Bytes(rootDocumentSlice)

			// The order of these args will be different for insert and update statements
			var sqlArgs []interface{}
			var stmtType string // so we can be specific in an error message
			if isUpdate {
				sqlArgs = append(sqlArgs, valuesArgs...)
				sqlArgs = append(sqlArgs, jsonBytes)
				sqlArgs = append(sqlArgs, keyArgs...)

				stmtType = "update"
				result, err := updateStatement.ExecContext(ctx, sqlArgs...)
				// Sanity check to ensure that we've updating a single row
				nRows, err := result.RowsAffected()
				if err != nil {
					return fmt.Errorf("Failed to get number of affected rows: %w", err)
				}
				if nRows != 1 {
					return fmt.Errorf("Expected exactly 1 row modified, got: %d", nRows)
				}
			} else {
				sqlArgs = append(sqlArgs, keyArgs...)
				sqlArgs = append(sqlArgs, valuesArgs...)
				sqlArgs = append(sqlArgs, jsonBytes)

				stmtType = "insert"
				result, err = insertStatement.ExecContext(ctx, sqlArgs...)
			}
			if err != nil {
				return fmt.Errorf("Failed to execute %s statement: %w", stmtType, err)
			}
		}
	}
}

// SQLDriver implementation details

func (driver *SQLDriver) getCachedSQL(ctx context.Context, handle *Handle) (*cachedSQL, error) {
	// We could alternatively use a concurrent map and a separate mutex per CallerId, but I decided
	// to KISS for now since it's doubtful that the simple single mutex will actually cause a
	// problem.
	driver.sqlCacheMutex.Lock()
	defer driver.sqlCacheMutex.Unlock()
	var cachedSQL = driver.sqlCache[handle.ShardID]
	// Do we need to re-create the sql queries?
	if cachedSQL == nil || cachedSQL.nonce != handle.Nonce {
		var newSQL, err = driver.generateRuntimeSQL(ctx, handle)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate sql statements for '%s': %w", handle.ShardID, err)
		}
		log.WithFields(log.Fields{
			"shardId": handle.ShardID,
			"nonce":   handle.Nonce,
		}).Debugf("Generated new sql statements: %+v", newSQL)
		driver.sqlCache[handle.ShardID] = newSQL
		cachedSQL = newSQL
	}
	return cachedSQL, nil
}

// Creates the sql used for Load/Store functions.
func (driver *SQLDriver) generateRuntimeSQL(ctx context.Context, handle *Handle) (*cachedSQL, error) {
	var materializationSpec, err = driver.loadMaterializationSpec(ctx, handle)
	if err != nil {
		return nil, err
	}
	var targetTable = tableForMaterialization(handle.Table, "", materializationSpec)

	loadQuery, err := driver.SQLGen.QueryOnPrimaryKey(targetTable, materializationSpec.Fields.Document)
	if err != nil {
		return nil, err
	}
	insertStatement, err := driver.SQLGen.InsertStatement(targetTable)
	if err != nil {
		return nil, err
	}

	var setColumns []string
	setColumns = append(setColumns, materializationSpec.Fields.Values...)
	setColumns = append(setColumns, materializationSpec.Fields.Document)

	updateStatement, err := driver.SQLGen.UpdateStatement(targetTable, setColumns, materializationSpec.Fields.Keys)
	if err != nil {
		return nil, err
	}

	return &cachedSQL{
		nonce:           handle.Nonce,
		loadQuery:       loadQuery,
		insertStatement: insertStatement,
		updateStatement: updateStatement,
	}, nil
}

// execTransaction is used during Apply to execute a sequence of non-parameterized statements within
// a single transaction.
func (driver *SQLDriver) execTransaction(ctx context.Context, handle *Handle, statements []string) error {
	var connection, err = driver.connection(handle.URI)
	if err != nil {
		return err
	}
	txn, err := connection.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	for _, sql := range statements {
		_, err = txn.ExecContext(ctx, sql)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"targetTtable": handle.Table,
				"shardId":      handle.ShardID,
			}).Warnf("Failed to execute SQL for applying materialization")
			_ = txn.Rollback()
			return err
		}
	}
	return txn.Commit()
}

func (driver *SQLDriver) doValidate(ctx context.Context, handle *Handle, proposed *pf.CollectionSpec, currentSpec *MaterializationSpec) (map[string]*pm.Constraint, error) {
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

// loadMaterializationSpec tries to query the existing MaterializationSpec from the
// flow_materializations table. This will return nil if now materialization spec is found or if any
// other error is encountered while executing the query. Ugh, I know. It's possible, or maybe
// likely, that an error here is simply because the flow_materializations table doesn't exist. This
// is perfectly normal if we've never materialized anything into this database before. Of course it
// could be caused by something else, in which case we will now proceed to return validation results
// that are potentially incorrect. To try to mitigate this possibility, we call `Ping` and return
// any error encountered there. The idea being that connection issues should not present as a
// missing materializationSpec. I'm assuming we don't want to create the table unless explicitly
// requested to, since people can be fussy about tools modifying their schemas.
func (driver *SQLDriver) loadMaterializationSpec(ctx context.Context, handle *Handle) (*MaterializationSpec, error) {
	var query, err = driver.SQLGen.QueryOnPrimaryKey(FlowMaterializationsTable(), FlowMaterializationsSpecColumn)
	if err != nil {
		return nil, err
	}

	connection, err := driver.connection(handle.URI)
	if err != nil {
		return nil, err
	}
	err = connection.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	var row = connection.QueryRowContext(ctx, query, handle.Table)
	// To keep things simple for now, we always store json as strings in text columns. We may allow
	// more flexibility later, though, in which case we'll have to accept either string or []byte
	// from the database here.
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

func (driver *SQLDriver) connection(endpointURI string) (*sql.DB, error) {
	driver.connectionsMutex.Lock()
	defer driver.connectionsMutex.Unlock()

	if db, ok := driver.connections[endpointURI]; ok {
		return db, nil
	}

	var db, err = sql.Open(driver.SQLDriverType, endpointURI)
	if err != nil {
		return nil, err
	}
	driver.connections[endpointURI] = db
	return db, nil
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
