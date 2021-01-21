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
	// The postgresql driver
	_ "github.com/lib/pq"
)

// Transaction represents a database transaction, which will correspond one-to-one with a
// transaction rpc.
type Transaction interface {
	// AddLoadKey adds the given key to the query.
	AddLoadKey(ctx context.Context, key []interface{}) error
	// PollLoadResults is called after adding the load keys for each LoadRequest, and
	// may return result documents by adding them to the given arena and returning a Slice for each
	// document. Nothing should be returned for a document that does not exist.
	PollLoadResults(ctx context.Context, arena *pf.Arena) ([]pf.Slice, error)

	// FlushLoadResults is called after each LoadEOF message, as this is the last opportunity to
	// return any documents that were loaded.
	FlushLoadResults(ctx context.Context, arena *pf.Arena) ([]pf.Slice, error)

	// Insert is called for each row in each StoreRequest message where `exists` is `false`
	Insert(ctx context.Context, args []interface{}) error

	// Update is called for each row in each StoreRequest message where `exists` is `true`
	Update(ctx context.Context, args []interface{}) error

	// Commit is called at the very end to a transaction, as long as no errors have been returned.
	Commit(ctx context.Context) error
	// Rollback is called if an error occurs as any point during an open transaction.
	Rollback() error
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

// ConnectionManager hands out Connections for each Handle.
type ConnectionManager interface {
	// Connection returns a Connection for the given Handle.
	Connection(ctx context.Context, handle *Handle) (Connection, error)
}

// CacheingConnectionManager caches each Connection, using the handle URI as a key. Currently, this
// cache is never invalidated, as each Connection is assumed to represent a pool of connections for
// a specific URI.
type CacheingConnectionManager struct {
	inner ConnectionManager
	// map of connection URI to DB for that system. We don't ever remove anythign from this map, so
	// it'll just keep growing if someone makes a bunch of requests for many distinct endpoints.
	connections      map[string]Connection
	connectionsMutex sync.Mutex
}

// NewCache returns a CacheingConnectionManager that wraps the given inner connectionManager and
// caches each returned Connection, using the URI as a key.
func NewCache(inner ConnectionManager) *CacheingConnectionManager {
	return &CacheingConnectionManager{
		inner:       inner,
		connections: make(map[string]Connection),
	}
}

// Connection implements the ConnectionManager interface
func (c *CacheingConnectionManager) Connection(ctx context.Context, handle *Handle) (Connection, error) {
	c.connectionsMutex.Lock()
	defer c.connectionsMutex.Unlock()

	if db, ok := c.connections[handle.URI]; ok {
		return db, nil
	}

	var db, err = c.inner.Connection(ctx, handle)
	if err != nil {
		return nil, err
	}
	c.connections[handle.URI] = db
	return db, nil
}

// A GenericDriver implements the DriverServer interface using the Connection, Transaction,
// ConnectionManager, and SQLGen interfaces.
type GenericDriver struct {
	// EndpointType is the type of the endpoint from the flow.yaml spec
	EndpointType string
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

	conn, err := g.Connections.Connection(ctx, handle)
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
	connection, err := g.Connections.Connection(ctx, handle)
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

// Transaction is part of the DriverServer implementation
func (g *GenericDriver) Transaction(stream pm.Driver_TransactionServer) (retErr error) {
	log.Debug("on Transaction start")
	var committed = false
	var ctx = stream.Context()
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

	conn, err := g.Connections.Connection(ctx, handle)
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

	defer func() {
		if retErr != nil && !committed {
			var rbErr = transaction.Rollback()
			logEntry.WithField("error", retErr).Warnf("Rolled back failed transaction with result: %v", rbErr)
		} else if retErr != nil {
			// If committed is true, then the error must have been returned from the call to commit
			logEntry.WithField("error", retErr).Warnf("Failed to commit Store transaction")
		} else {
			logEntry.Debug("Successfully committed Store transaction")
		}
	}()

	var loadResponseNum = 0
	// Handle all the Loads
	var responseArena pf.Arena
	for {
		req, err = stream.Recv()
		if err != nil {
			return err // EOF is not expected here, so we'd return is as an error.
		}

		if req.Load != nil {
			logEntry.WithField("numDocs", len(req.Load.PackedKeys)).Trace("got load request")
			for _, key := range req.Load.PackedKeys {
				tup, err := tuple.Unpack(req.Load.Arena.Bytes(key))
				if err != nil {
					return err
				}
				args, err := cachedSQL.QueryKeyConverter.Convert(tup.ToInterface()...)
				if err != nil {
					return err
				}
				err = transaction.AddLoadKey(ctx, args)
				if err != nil {
					return err
				}
			}

			// Poll to see whether we'll send a LoadResponse now
			slices, err := transaction.PollLoadResults(ctx, &responseArena)
			if err != nil {
				return fmt.Errorf("failed to poll load results: %w", err)
			}
			if len(slices) > 0 {
				loadResponseNum++
				logEntry.Trace("sending load response: ", loadResponseNum)
				var response = pm.TransactionResponse_LoadResponse{
					Arena:    responseArena,
					DocsJson: slices,
				}
				err = stream.Send(&pm.TransactionResponse{
					LoadResponse: &response,
				})
				if err != nil {
					return fmt.Errorf("failed to send load repsonse")
				}
				// Since we sent a response, truncate the response arena for later re-use.
				responseArena = responseArena[:0]
			}
		} else {
			// If Load == nil, then the next message must be a LoadEOF
			if req.LoadEOF == nil {
				return fmt.Errorf("expected either a Load or LoadEOF message")
			}
			// We're done loading, so it's time to flush the Loading stage of the transaction. This
			// will be the final LoadResponse for this transaction, and it might also be the only
			// one.
			slices, err := transaction.FlushLoadResults(ctx, &responseArena)
			if err != nil {
				return fmt.Errorf("failed to flush load results: %w", err)
			}
			if len(slices) > 0 {
				loadResponseNum++
				logEntry.Trace("sending final load response: ", loadResponseNum)
				var response = pm.TransactionResponse_LoadResponse{
					Arena:    responseArena,
					DocsJson: slices,
				}
				err = stream.Send(&pm.TransactionResponse{
					LoadResponse: &response,
				})
				if err != nil {
					return fmt.Errorf("failed to send load repsonse")
				}
			}

			// We're done sending LoadResponses, so now we send the LoadEOF
			err = stream.Send(&pm.TransactionResponse{
				LoadEOF: &pm.LoadEOF{},
			})
			if err != nil {
				return fmt.Errorf("failed to send loadEOF")
			}
			break
		}
	}

	// Time to handle some Stores!
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			err = nil // So we don't treat this as an error condition
			break
		} else if err != nil {
			return err
		}
		if req.Store == nil {
			return fmt.Errorf("expected Store message")
		}

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
			var args []interface{}
			// Are we doing an update or an insert?
			// Note that the order of arguments is different for inserts vs updates.
			if exists {
				args = append(args, values.ToInterface()...)
				args = append(args, docJson)
				args = append(args, key.ToInterface()...)

				convertedValues, err := cachedSQL.UpdateValuesConverter.Convert(args...)
				if err != nil {
					return err
				}
				err = transaction.Update(ctx, convertedValues)
				if err != nil {
					return err
				}
			} else {
				args = append(args, key.ToInterface()...)
				args = append(args, values.ToInterface()...)
				args = append(args, docJson)

				convertedValues, err := cachedSQL.InsertValuesConverter.Convert(args...)
				if err != nil {
					return err
				}
				err = transaction.Insert(ctx, convertedValues)
				if err != nil {
					return err
				}
			}
		}
	}

	logEntry.Debug("Committing transaction")
	// At this point, we've gotten an EOF, so we're done processing Store requests.
	// It's time to commit the transaction and send a Store response.
	err = transaction.Commit(ctx)
	committed = true
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	err = stream.Send(&pm.TransactionResponse{
		StoreResponse: &pm.TransactionResponse_StoreResponse{},
	})
	if err != nil {
		return fmt.Errorf("failed to send StoreResponse after transaction commit: %w", err)
	}
	return nil
}

// Apply is part of the DriverServer implementation.
func (g *GenericDriver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var handle, err = parseHandle(req.Handle)
	if err != nil {
		return nil, err
	}
	conn, err := g.Connections.Connection(ctx, handle)
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
