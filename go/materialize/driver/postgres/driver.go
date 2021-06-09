package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	pgxStd "github.com/jackc/pgx/v4/stdlib"
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

// Validate the configuration.
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

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
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
		NewEndpoint: func(ctx context.Context, name string, config json.RawMessage) (*sqlDriver.Endpoint, error) {
			var parsed = new(Config)

			if err := json.Unmarshal(config, parsed); err != nil {
				return nil, fmt.Errorf("parsing Postgresql configuration: %w", err)
			} else if err = parsed.Validate(); err != nil {
				return nil, fmt.Errorf("configuration is invalid: %w", err)
			}

			db, err := sql.Open("pgx", parsed.ToURI())
			if err != nil {
				return nil, fmt.Errorf("opening Postgres database: %w", err)
			}

			var endpoint = &sqlDriver.Endpoint{
				Config:       parsed,
				Context:      ctx,
				Name:         name,
				DB:           db,
				DeltaUpdates: false, // TODO: supporting deltas requires relaxing CREATE TABLE generation.
				TablePath:    []string{parsed.DBName, parsed.User, parsed.Table},
				Generator:    sqlDriver.PostgresSQLGenerator(),
			}
			endpoint.Tables.Checkpoints = sqlDriver.FlowCheckpointsTable(sqlDriver.DefaultFlowCheckpoints)
			endpoint.Tables.Specs = sqlDriver.FlowMaterializationsTable(sqlDriver.DefaultFlowMaterializations)

			return endpoint, nil
		},
		NewTransactor: func(ep *sqlDriver.Endpoint, spec *pf.MaterializationSpec, fence *sqlDriver.Fence) (lifecycle.Transactor, error) {
			var err error
			var target = sqlDriver.TableForMaterialization(ep.TargetName(), "", &ep.Generator.IdentifierQuotes, spec)
			var d = &transactor{ctx: ep.Context}

			// Build all SQL statements and parameter converters.
			var keyCreateSQL string
			keyCreateSQL, d.load.query.sql, err = BuildSQL(&ep.Generator, target, spec.FieldSelection)
			if err != nil {
				return nil, fmt.Errorf("building SQL: %w", err)
			}

			d.load.keys = spec.FieldSelection.Keys
			_, d.load.params, err = ep.Generator.QueryOnPrimaryKey(target, spec.FieldSelection.Document)
			if err != nil {
				return nil, fmt.Errorf("building load SQL: %w", err)
			}
			d.store.insert.sql, d.store.insert.params, err = ep.Generator.InsertStatement(target)
			if err != nil {
				return nil, fmt.Errorf("building insert SQL: %w", err)
			}
			d.store.update.sql, d.store.update.params, err = ep.Generator.UpdateStatement(
				target,
				append(append([]string{}, spec.FieldSelection.Values...), spec.FieldSelection.Document),
				spec.FieldSelection.Keys)
			if err != nil {
				return nil, fmt.Errorf("building update SQL: %w", err)
			}

			// Establish connections.
			if d.load.conn, err = pgxStd.AcquireConn(ep.DB); err != nil {
				return nil, fmt.Errorf("load pgx.AcquireConn: %w", err)
			}
			if d.store.conn, err = pgxStd.AcquireConn(ep.DB); err != nil {
				return nil, fmt.Errorf("store pgx.AcquireConn: %w", err)
			}

			// Create session-scoped temporary table for key loads.
			if _, err = d.load.conn.Exec(d.ctx, keyCreateSQL); err != nil {
				return nil, fmt.Errorf("Exec(%s): %w", keyCreateSQL, err)
			}
			// Prepare query statements.
			for _, t := range []struct {
				conn *pgx.Conn
				name string
				sql  string
				stmt **pgconn.StatementDescription
			}{
				{d.load.conn, "load-join", d.load.query.sql, &d.load.query.stmt},
				{d.store.conn, "store-insert", d.store.insert.sql, &d.store.insert.stmt},
				{d.store.conn, "store-update", d.store.update.sql, &d.store.update.stmt},
			} {
				*t.stmt, err = t.conn.Prepare(d.ctx, t.name, t.sql)
				if err != nil {
					return nil, fmt.Errorf("conn.PrepareContext(%s): %w", t.sql, err)
				}
			}

			d.store.fence = fence

			return d, nil
		},
	}
}

type transactor struct {
	ctx context.Context
	// Variables exclusively used by Load.
	load struct {
		conn   *pgx.Conn
		params sqlDriver.ParametersConverter
		keys   []string
		query  struct {
			sql  string
			stmt *pgconn.StatementDescription
		}
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		batch  *pgx.Batch
		conn   *pgx.Conn
		fence  *sqlDriver.Fence
		insert struct {
			sql    string
			stmt   *pgconn.StatementDescription
			params sqlDriver.ParametersConverter
		}
		update struct {
			sql    string
			stmt   *pgconn.StatementDescription
			params sqlDriver.ParametersConverter
		}
	}
}

func (d *transactor) Load(it *lifecycle.LoadIterator, _ <-chan struct{}, loaded func(json.RawMessage) error) error {
	var txn, err = d.load.conn.BeginTx(d.ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback(d.ctx)

	var source = keyCopySource{
		params:       d.load.params,
		LoadIterator: it,
	}
	_, err = txn.CopyFrom(d.ctx, pgx.Identifier{tempTableName}, d.load.keys, &source)
	if err != nil {
		return fmt.Errorf("copying Loads to temp table: %w", err)
	} else if err := it.Err(); err != nil {
		return err
	}

	// Query the documents, joining with the temp table.
	rows, err := txn.Query(d.ctx, d.load.query.stmt.Name)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var document json.RawMessage

		if err = rows.Scan(&document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	} else if err = txn.Commit(d.ctx); err != nil {
		return fmt.Errorf("commiting Load transaction: %w", err)
	}

	return nil
}

func (d *transactor) Prepare(prepare *pm.TransactionRequest_Prepare) (_ *pm.TransactionResponse_Prepared, err error) {
	d.store.fence.Checkpoint = prepare.FlowCheckpoint
	d.store.batch = new(pgx.Batch)

	return &pm.TransactionResponse_Prepared{
		DriverCheckpointJson: nil, // Not used.
	}, nil
}

func (d *transactor) Store(it *lifecycle.StoreIterator) error {
	for it.Next() {
		if it.Exists {
			converted, err := d.store.update.params.Convert(
				append(append(it.Values, it.RawJSON), it.Key...))
			if err != nil {
				return fmt.Errorf("converting update parameters: %w", err)
			}
			d.store.batch.Queue(d.store.update.stmt.Name, converted...)
		} else {
			converted, err := d.store.insert.params.Convert(
				append(append(it.Key, it.Values...), it.RawJSON))
			if err != nil {
				return fmt.Errorf("converting insert parameters: %w", err)
			}
			d.store.batch.Queue(d.store.insert.stmt.Name, converted...)
		}
	}
	return nil
}

func (d *transactor) Commit() error {
	var txn, err = d.store.conn.BeginTx(d.ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("conn.BeginTx: %w", err)
	}
	defer txn.Rollback(d.ctx)

	err = d.store.fence.Update(
		func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
			// Add the update to the fence as the last statement in the batch
			var docs = d.store.batch.Len()
			d.store.batch.Queue(sql, args...)

			var results = txn.SendBatch(d.ctx, d.store.batch)
			d.store.batch = nil

			for i := 0; i != docs; i++ {
				if _, err := results.Exec(); err != nil {
					return 0, fmt.Errorf("store at index %d: %w", i, err)
				}
			}

			// The fence update is always the last operation in the batch
			fenceResult, err := results.Exec()
			if err != nil {
				return 0, fmt.Errorf("updating flow checkpoint: %w", err)
			} else if err = results.Close(); err != nil {
				return 0, fmt.Errorf("results.Close(): %w", err)
			}

			return fenceResult.RowsAffected(), nil
		})
	if err != nil {
		return err
	}

	if err := txn.Commit(d.ctx); err != nil {
		return fmt.Errorf("committing Store transaction: %w", err)
	}

	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close(d.ctx)
	d.store.conn.Close(d.ctx)
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

// BuildSQL builds SQL statements use for PostgreSQL materializations.
func BuildSQL(gen *sqlDriver.Generator, table *sqlDriver.Table, fields pf.FieldSelection) (
	keyCreate, keyJoin string, err error) {

	var defs, joins []string
	for _, key := range fields.Keys {
		var col = table.GetColumn(key)
		var resolved *sqlDriver.ResolvedColumnType

		if resolved, err = gen.TypeMappings.GetColumnType(col); err != nil {
			return
		}

		// CREATE TABLE column definitions.
		defs = append(defs,
			fmt.Sprintf("%s %s",
				col.Identifier,
				resolved.SQLType,
			),
		)
		// JOIN constraints.
		joins = append(joins, fmt.Sprintf("l.%s = r.%s", col.Identifier, col.Identifier))
	}

	// CREATE temporary table which queues keys to load.
	keyCreate = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			%s
		) ON COMMIT DELETE ROWS
		;`,
		tempTableName,
		strings.Join(defs, ", "),
	)

	// SELECT documents included in keys to load.
	keyJoin = fmt.Sprintf(`
		SELECT l.%s
			FROM %s AS l
			JOIN %s AS r
			ON %s
		;`,
		table.GetColumn(fields.Document).Identifier,
		table.Identifier,
		tempTableName,
		strings.Join(joins, " AND "),
	)

	return
}

const tempTableName = "flow_load_key_tmp"
