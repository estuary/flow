package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgx/v4"
	pgxStd "github.com/jackc/pgx/v4/stdlib"
	log "github.com/sirupsen/logrus"
)

// Config represents the merged endpoint configuration for connections to postgres.
// This struct definition must match the one defined for the source specs (flow.yaml) in Rust.
type Config struct {
	Host     string
	Port     uint16
	User     string
	Password string
	DBName   string
	Table    string
}

func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"table", c.Table},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing database configuration property: '%s'", req[0])
		}
	}
	return nil
}

func (c *Config) ToUri() string {
	var host = c.Host
	if c.Port != 0 {
		host = fmt.Sprintf("%s:%d", host, c.Port)
	}
	var uri = url.URL{
		Scheme: "postgres",
		Host:   host,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.DBName != "" {
		uri.Path = "/" + c.DBName
	}
	return uri.String()
}

// NewPostgresDriver creates a new Driver for postgresql.
func NewPostgresDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		NewEndpoint: func(ctx context.Context, et pf.EndpointType, config json.RawMessage) (*sqlDriver.Endpoint, error) {
			var parsed Config

			if err := json.Unmarshal(config, &parsed); err != nil {
				return nil, fmt.Errorf("parsing Postgresql configuration: %w", err)
			}
			if err := parsed.Validate(); err != nil {
				return nil, fmt.Errorf("Postgres configuration is invalid: %w", err)
			}

			db, err := sql.Open("pgx", parsed.ToUri())
			if err != nil {
				return nil, fmt.Errorf("opening Postgres database: %w", err)
			}

			var endpoint = &sqlDriver.Endpoint{
				Context:      ctx,
				EndpointType: et,
				DB:           db,
				Generator:    sqlDriver.PostgresSQLGenerator(),
			}
			endpoint.Tables.Target = parsed.Table
			endpoint.Tables.Checkpoints = sqlDriver.DefaultGazetteCheckpoints
			endpoint.Tables.Specs = sqlDriver.DefaultFlowMaterializations

			return endpoint, nil
		},
		RunTransactions: runPostgresTransactions,
	}
}

func runPostgresTransactions(stream pm.Driver_TransactionsServer, endpoint *sqlDriver.Endpoint, spec *pf.MaterializationSpec, fence *sqlDriver.Fence) error {
	var logEntry = fence.LogEntry()

	var target = sqlDriver.TableForMaterialization(endpoint.Tables.Target, "", spec)
	var _, keyParams, err = endpoint.Generator.QueryOnPrimaryKey(target, spec.FieldSelection.Document)
	if err != nil {
		return fmt.Errorf("generating key parameter converter: %w", err)
	}
	var loadSQL = strings.Join([]string{
		"SELECT",
		spec.FieldSelection.Document,
		"FROM",
		tempKeyTableName,
		"NATURAL JOIN",
		endpoint.Tables.Target,
		";",
	}, " ")
	insertSQL, insertParams, err := endpoint.Generator.InsertStatement(target)
	if err != nil {
		return fmt.Errorf("generating insert statement: %w", err)
	}
	updateSql, updateParams, err := endpoint.Generator.UpdateStatement(target, append(spec.FieldSelection.Values, spec.FieldSelection.Document), spec.FieldSelection.Keys)

	conn, err := pgxStd.AcquireConn(endpoint.DB)
	if err != nil {
		return fmt.Errorf("acquiring postgres connection: %w", err)
	}
	defer pgxStd.ReleaseConn(endpoint.DB, conn)

	var tempTable = loadKeyTempTable(spec)
	createTemp, err := endpoint.Generator.CreateTable(tempTable)
	if err != nil {
		return fmt.Errorf("generating temp table sql: %w", err)
	}
	_, err = conn.Exec(endpoint.Context, createTemp)
	if err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}

	var txn pgx.Tx
	defer func() {
		if txn != nil {
			_ = txn.Rollback(endpoint.Context) // Best-effort rollback.
		}
	}()

	var response *pm.TransactionResponse
	for {
		if txn, err = conn.BeginTx(endpoint.Context, pgx.TxOptions{}); err != nil {
			return fmt.Errorf("DB.BeginTx: %w", err)
		}
		var loadIt = lifecycle.NewLoadIterator(stream)
		// Did the client send at least one Load request?
		if loadIt.Poll() {
			var loadKeys = keyCopySource{
				params:       keyParams,
				LoadIterator: loadIt,
			}
			numKeys, err := txn.CopyFrom(endpoint.Context, pgx.Identifier{tempKeyTableName}, spec.FieldSelection.Keys, &loadKeys)
			if err != nil {
				return fmt.Errorf("copying keys to temp table: %w", err)
			}

			// Query the documents, joining with the temp table
			var foundDocs int
			rows, err := txn.Query(endpoint.Context, loadSQL)
			if err != nil {
				return fmt.Errorf("querying documents: %w", err)
			}

			for rows.Next() {
				foundDocs++
				var json json.RawMessage
				if err = rows.Scan(&json); err != nil {
					return fmt.Errorf("reading query result row %d: %w", foundDocs, err)
				}
				lifecycle.StageLoaded(stream, &response, json)
			}
			rows.Close()
			logEntry.WithFields(log.Fields{
				"requestedKeys": numKeys,
				"loadedDocs":    foundDocs,
			}).Debug("finished loading documents")
		}
		err = loadIt.Err()
		if err == io.EOF {
			logEntry.Debug("End of fenced transactions")
			// If there's an in-progress transaction, then it will be rolled back
			return nil
		} else if err != nil {
			return fmt.Errorf("reading Load requests: %w", err)
		}
		var prepare = loadIt.Prepare()
		if err = lifecycle.WritePrepared(stream, &response, nil); err != nil {
			return err
		}

		// Prepare our insert and update statements. These must be referenced by name when
		// operations are queued to the batch.
		if _, err := txn.Prepare(endpoint.Context, "insert", insertSQL); err != nil {
			return fmt.Errorf("preparing insert statement: %w", err)
		}

		if _, err = txn.Prepare(endpoint.Context, "update", updateSql); err != nil {
			return fmt.Errorf("preparing update statement: %w", err)
		}
		var batch = &pgx.Batch{}
		var storeIt = lifecycle.NewStoreIterator(stream)
		var stored = 0
		for storeIt.Next() {
			stored++
			// Will this be an update or an insert?
			if storeIt.Exists {
				updateArgs, err := updateParams.Convert(append(append(storeIt.Values, storeIt.RawJSON), storeIt.Key...))
				if err != nil {
					return fmt.Errorf("converting update parameters: %w", err)
				}
				batch.Queue("update", updateArgs...)
			} else {
				insertArgs, err := insertParams.Convert(append(append(
					storeIt.Key, storeIt.Values...), storeIt.RawJSON))
				if err != nil {
					return fmt.Errorf("converting insert parameters: %w", err)
				}
				batch.Queue("insert", insertArgs...)
			}
		}
		if storeIt.Err() != nil {
			return fmt.Errorf("reading store requests: %w", storeIt.Err())
		}

		fence.Checkpoint = prepare.FlowCheckpoint
		err = fence.Update(func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
			// Add the update to the fence as the last statement in the batch
			batch.Queue(sql, args...)

			logEntry.WithField("nDocs", stored).Debug("Sending batch")
			var batchResults = txn.SendBatch(endpoint.Context, batch)
			if err != nil {
				return 0, fmt.Errorf("sending batch: %w", err)
			}
			// Return an error if any of the insert or update operations failed
			for i := 0; i < stored; i++ {
				_, err := batchResults.Exec()
				if err != nil {
					return 0, fmt.Errorf("executing store at index %d: %w", i, err)
				}
			}
			// The fence update is always the last operation in the batch
			fenceResult, err := batchResults.Exec()
			if err != nil {
				return 0, fmt.Errorf("updating flow checkpoint: %w", err)
			}
			err = batchResults.Close()
			if err != nil {
				return 0, fmt.Errorf("closing batch results: %w", err)
			}

			return fenceResult.RowsAffected(), nil
		})
		if err != nil {
			return err
		}

		logEntry.Debug("Committing transaction")
		err = txn.Commit(endpoint.Context)
		txn = nil // So that we don't try to rollback the transaction
		if err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		if err = lifecycle.WriteCommitted(stream, &response); err != nil {
			return fmt.Errorf("sending WriteCommitted response after successful commit: %w", err)
		}
	}
}

// keyCopySource adapts a LoadIterator to the pgx.CopyFromSource interface, which allows copying
// keys into the temp table. The Next and Err functions are provided by LoadIterator.
type keyCopySource struct {
	*lifecycle.LoadIterator
	params sqlDriver.ParametersConverter
}

// Values is part of the pgx.CopyFromSource implementation
func (t *keyCopySource) Values() ([]interface{}, error) {
	return t.params.Convert(t.Key)
}

const tempKeyTableName = "flow_load_key_tmp"

func loadKeyTempTable(spec *pf.MaterializationSpec) *sqlDriver.Table {
	var columns = make([]sqlDriver.Column, len(spec.FieldSelection.Keys))
	for i, keyField := range spec.FieldSelection.Keys {
		var projection = spec.Collection.GetProjection(keyField)
		columns[i] = sqlDriver.ColumnForProjection(projection)
	}
	return &sqlDriver.Table{
		Name:         tempKeyTableName,
		Columns:      columns,
		IfNotExists:  true,
		Temporary:    true,
		TempOnCommit: "DELETE ROWS",
	}
}
