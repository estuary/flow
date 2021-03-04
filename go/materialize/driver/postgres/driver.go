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

// PostgresConfig represents the merged endpoint configuration for connections to postgres.
// This struct definition must match the one defined for the source specs (flow.yaml) in Rust.
type PostgresConfig struct {
	Host     string
	Port     uint16
	User     string
	Password string
	DBName   string
	Table    string
}

func (c *PostgresConfig) Validate() error {
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

func (c *PostgresConfig) ToUri() string {
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
			var parsed PostgresConfig

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
	var logEntry = log.WithFields(log.Fields{
		"shardId": fence.ShardFqn,
		"fence":   fence.Fence,
	})

	var targetTable = sqlDriver.TableForMaterialization(endpoint.Tables.Target, "", spec)
	var _, keyParamsConverter, err = endpoint.Generator.QueryOnPrimaryKey(targetTable, spec.FieldSelection.Document)
	if err != nil {
		return fmt.Errorf("generating key parameter converter: %w", err)
	}
	var loadJoinQuery = strings.Join([]string{
		"SELECT",
		spec.FieldSelection.Document,
		"FROM",
		tempKeyTableName,
		"NATURAL JOIN",
		endpoint.Tables.Target,
		";",
	}, " ")
	insertSql, insertParamConverter, err := endpoint.Generator.InsertStatement(targetTable)
	if err != nil {
		return fmt.Errorf("generating insert statement: %w", err)
	}
	var updateFields = spec.FieldSelection.Values
	updateFields = append(updateFields, spec.FieldSelection.Document)
	updateSql, updateParamConverter, err := endpoint.Generator.UpdateStatement(targetTable, updateFields, spec.FieldSelection.Keys)

	conn, err := pgxStd.AcquireConn(endpoint.DB)
	if err != nil {
		return fmt.Errorf("acquiring postgres connection: %w", err)
	}
	defer pgxStd.ReleaseConn(endpoint.DB, conn)

	var tempKeyTable = loadKeyTempTable(spec)
	createTemp, err := endpoint.Generator.CreateTable(tempKeyTable)
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
		var loadIterator = lifecycle.NewLoadIterator(stream)
		// Did the client send at least one Load request?
		if loadIterator.Poll() {
			var loadKeys = pgKeyLoader{
				paramConverter: keyParamsConverter,
				LoadIterator:   loadIterator,
			}
			numKeys, err := txn.CopyFrom(endpoint.Context, pgx.Identifier{tempKeyTableName}, spec.FieldSelection.Keys, &loadKeys)
			if err != nil {
				return fmt.Errorf("copying keys to temp table: %w", err)
			}

			// Query the documents, joining with the temp table
			var foundDocs int
			rows, err := txn.Query(endpoint.Context, loadJoinQuery)
			if err != nil {
				return fmt.Errorf("querying documents: %w", err)
			}

			for rows.Next() {
				foundDocs++
				var json json.RawMessage
				err = rows.Scan(&json)
				if err != nil {
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
		err = loadIterator.Err()
		if err == io.EOF {
			logEntry.Debug("End of fenced transactions")
			// If there's an in-progress transaction, then it will be rolled back
			return nil
		} else if err != nil {
			return fmt.Errorf("reading Load requests: %w", err)
		}
		var prepare = loadIterator.Prepare()
		fence.Checkpoint = prepare.FlowCheckpoint
		fence.Update(func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
			if result, fenceErr := txn.Exec(ctx, sql, args...); fenceErr != nil {
				return 0, fmt.Errorf("updating flow checkpoint: %w", fenceErr)
			} else {
				return result.RowsAffected(), nil
			}
		})

		logEntry.Debug("Writing Prepared")
		err = lifecycle.WritePrepared(stream, &response, nil)
		if err != nil {
			return err
		}

		// Prepare our insert and update statements. These must be referenced by name when
		// operations are queued to the batch.
		_, err := txn.Prepare(endpoint.Context, "insert", insertSql)
		if err != nil {
			return fmt.Errorf("preparing insert statement: %w", err)
		}
		_, err = txn.Prepare(endpoint.Context, "update", updateSql)
		if err != nil {
			return fmt.Errorf("preparing update statement: %w", err)
		}
		var batch = &pgx.Batch{}
		var storeIter = lifecycle.NewStoreIterator(stream)
		var storedDocs = 0
		for storeIter.Next() {
			storedDocs++
			// Will this be an update or an insert?
			if storeIter.Exists {
				updateParams, err := updateParamConverter.Convert(append(append(storeIter.Values, storeIter.RawJSON), storeIter.Key...))
				if err != nil {
					return fmt.Errorf("converting update parameters: %w", err)
				}
				batch.Queue("update", updateParams...)
			} else {
				insertParams, err := insertParamConverter.Convert(append(append(
					storeIter.Key, storeIter.Values...), storeIter.RawJSON))
				if err != nil {
					return fmt.Errorf("converting insert parameters: %w", err)
				}
				batch.Queue("insert", insertParams...)
			}
		}
		if storeIter.Err() != nil {
			return fmt.Errorf("reading store requests: %w", storeIter.Err())
		}

		// Skip sending the batch if the client didn't send any Store requests
		if storedDocs > 0 {
			logEntry.WithField("nDocs", storedDocs).Debug("Sending batch")
			var batchResults = txn.SendBatch(endpoint.Context, batch)
			if err != nil {
				return fmt.Errorf("sending batch: %w", err)
			}
			for i := 0; i < storedDocs; i++ {
				_, err := batchResults.Exec()
				if err != nil {
					return fmt.Errorf("executing store at index %d: %w", i, err)
				}
			}
			err = batchResults.Close()
			if err != nil {
				return fmt.Errorf("closing batch results: %w", err)
			}
		}

		logEntry.Debug("Committing transaction")
		err = txn.Commit(endpoint.Context)
		txn = nil
		if err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		err = lifecycle.WriteCommitted(stream, &response)
		if err != nil {
			// This isn't really a problem since we will return the proper flow_checkpoint on the
			// next call to the Transactions rpc, but it seems worth logging.
			logEntry.Warnf("failed to send WriteCommitted response after a successful commit")
			return err
		}
	}
}

// pgKeyLoader adapts a LoadIterator to the pgx.CopyFromSource interface, which allows copying keys
// into the temp table
type pgKeyLoader struct {
	*lifecycle.LoadIterator
	paramConverter sqlDriver.ParametersConverter
}

// Values is part of the pgx.CopyFromSource implementation
func (t *pgKeyLoader) Values() ([]interface{}, error) {
	return t.paramConverter.Convert(t.Key)
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
