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
	log "github.com/sirupsen/logrus"
)

// config represents the endpoint configuration for postgres.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Host     string
	Port     uint16
	User     string
	Password string
	DBName   string
}

// Validate the configuration.
func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
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

type tableConfig struct {
	Table string
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) Path() sqlDriver.ResourcePath {
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return false // PostgreSQL doesn't support delta updates.
}

// NewPostgresDriver creates a new Driver for postgresql.
func NewPostgresDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		NewResource: func(*sqlDriver.Endpoint) sqlDriver.Resource { return new(tableConfig) },
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (*sqlDriver.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing Postgresql configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": parsed.DBName,
				"host":     parsed.Host,
				"port":     parsed.Port,
				"user":     parsed.User,
			}).Info("opening database")

			db, err := sql.Open("pgx", parsed.ToURI())
			if err != nil {
				return nil, fmt.Errorf("opening Postgres database: %w", err)
			}

			var endpoint = &sqlDriver.Endpoint{
				Config:    parsed,
				Context:   ctx,
				DB:        db,
				Generator: sqlDriver.PostgresSQLGenerator(),
			}
			endpoint.Tables.Checkpoints = sqlDriver.FlowCheckpointsTable(sqlDriver.DefaultFlowCheckpoints)
			endpoint.Tables.Specs = sqlDriver.FlowMaterializationsTable(sqlDriver.DefaultFlowMaterializations)

			return endpoint, nil
		},
		NewTransactor: func(
			ep *sqlDriver.Endpoint,
			spec *pf.MaterializationSpec,
			fence *sqlDriver.Fence,
			resources []sqlDriver.Resource,
		) (_ lifecycle.Transactor, err error) {
			var d = &transactor{
				ctx: ep.Context,
				gen: &ep.Generator,
			}
			d.store.fence = fence

			// Establish connections.
			if d.load.conn, err = pgxStd.AcquireConn(ep.DB); err != nil {
				return nil, fmt.Errorf("load pgx.AcquireConn: %w", err)
			}
			if d.store.conn, err = pgxStd.AcquireConn(ep.DB); err != nil {
				return nil, fmt.Errorf("store pgx.AcquireConn: %w", err)
			}

			for _, spec := range spec.Bindings {
				var target = sqlDriver.ResourcePath(spec.ResourcePath).Join()
				if err = d.addBinding(spec); err != nil {
					return nil, fmt.Errorf("%s: %w", target, err)
				}
			}

			// Build a query which unions the results of each load subquery.
			var subqueries []string
			for _, b := range d.bindings {
				subqueries = append(subqueries, b.load.query.sql)
			}
			var loadAllSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

			d.load.stmt, err = d.load.conn.Prepare(d.ctx, "load-join-all", loadAllSQL)
			if err != nil {
				return nil, fmt.Errorf("conn.PrepareContext(%s): %w", loadAllSQL, err)
			}

			return d, nil
		},
	}
}

type transactor struct {
	ctx context.Context
	gen *sqlDriver.Generator

	// Variables exclusively used by Load.
	load struct {
		conn *pgx.Conn
		stmt *pgconn.StatementDescription
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		batch *pgx.Batch
		conn  *pgx.Conn
		fence *sqlDriver.Fence
	}
	bindings []*binding
}

type binding struct {
	// Variables exclusively used by Load.
	load struct {
		params sqlDriver.ParametersConverter
		keys   []string
		insert struct {
			sql  string
			stmt *pgconn.StatementDescription
		}
		query struct {
			sql  string
			stmt *pgconn.StatementDescription
		}
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
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

func (t *transactor) addBinding(spec *pf.MaterializationSpec_Binding) error {
	var err error
	var bind = new(binding)
	var index = len(t.bindings)
	var target = sqlDriver.TableForMaterialization(strings.Join(spec.ResourcePath, "."), "", &t.gen.IdentifierQuotes, spec)

	// Build all SQL statements and parameter converters.
	var keyCreateSQL string
	keyCreateSQL, bind.load.insert.sql, bind.load.query.sql, err = BuildSQL(
		t.gen, index, target, spec.FieldSelection)
	if err != nil {
		return fmt.Errorf("building SQL: %w", err)
	}

	bind.load.keys = spec.FieldSelection.Keys
	_, bind.load.params, err = t.gen.QueryOnPrimaryKey(target, spec.FieldSelection.Document)
	if err != nil {
		return fmt.Errorf("building load SQL: %w", err)
	}
	bind.store.insert.sql, bind.store.insert.params, err = t.gen.InsertStatement(target)
	if err != nil {
		return fmt.Errorf("building insert SQL: %w", err)
	}
	bind.store.update.sql, bind.store.update.params, err = t.gen.UpdateStatement(
		target,
		append(append([]string{}, spec.FieldSelection.Values...), spec.FieldSelection.Document),
		spec.FieldSelection.Keys)
	if err != nil {
		return fmt.Errorf("building update SQL: %w", err)
	}

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err = t.load.conn.Exec(t.ctx, keyCreateSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", keyCreateSQL, err)
	}
	// Prepare query statements.
	for _, s := range []struct {
		conn *pgx.Conn
		name string
		sql  string
		stmt **pgconn.StatementDescription
	}{
		{
			t.load.conn,
			fmt.Sprintf("load-insert-%d", index),
			bind.load.insert.sql,
			&bind.load.insert.stmt,
		},
		{
			t.load.conn,
			fmt.Sprintf("load-join-%d", index),
			bind.load.query.sql,
			&bind.load.query.stmt,
		},
		{
			t.store.conn,
			fmt.Sprintf("store-insert-%d", index),
			bind.store.insert.sql,
			&bind.store.insert.stmt,
		},
		{
			t.store.conn,
			fmt.Sprintf("store-update-%d", index),
			bind.store.update.sql,
			&bind.store.update.stmt,
		},
	} {
		*s.stmt, err = s.conn.Prepare(t.ctx, s.name, s.sql)
		if err != nil {
			return fmt.Errorf("conn.PrepareContext(%s): %w", s.sql, err)
		}
	}

	t.bindings = append(t.bindings, bind)
	return nil
}

func (d *transactor) Load(it *lifecycle.LoadIterator, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	// Use a read-only "load" transaction, which will automatically
	// truncate the temporary key staging tables on commit.
	var txn, err = d.load.conn.BeginTx(d.ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	defer txn.Rollback(d.ctx)

	var batch pgx.Batch
	for it.Next() {
		var b = d.bindings[it.Binding]

		converted, err := b.load.params.Convert(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}
		batch.Queue(b.load.insert.stmt.Name, converted...)
	}
	if it.Err() != nil {
		return it.Err()
	}

	var results = txn.SendBatch(d.ctx, &batch)
	for i := 0; i != batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("load at index %d: %w", i, err)
		}
	}
	if err = results.Close(); err != nil {
		return fmt.Errorf("closing batch: %w", err)
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := txn.Query(d.ctx, d.load.stmt.Name)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document json.RawMessage

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
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
		var b = d.bindings[it.Binding]

		if it.Exists {
			converted, err := b.store.update.params.Convert(
				append(append(it.Values, it.RawJSON), it.Key...))
			if err != nil {
				return fmt.Errorf("converting update parameters: %w", err)
			}
			d.store.batch.Queue(b.store.update.stmt.Name, converted...)
		} else {
			converted, err := b.store.insert.params.Convert(
				append(append(it.Key, it.Values...), it.RawJSON))
			if err != nil {
				return fmt.Errorf("converting insert parameters: %w", err)
			}
			d.store.batch.Queue(b.store.insert.stmt.Name, converted...)
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

// BuildSQL builds SQL statements use for PostgreSQL materializations.
func BuildSQL(gen *sqlDriver.Generator, binding int, table *sqlDriver.Table, fields pf.FieldSelection) (
	keyCreate, keyInsert, keyJoin string, err error) {

	var defs, keys, keyPH, joins []string
	for idx, key := range fields.Keys {
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
		// INSERT key columns.
		keys = append(keys, col.Identifier)
		keyPH = append(keyPH, gen.Placeholder(idx))

		// JOIN constraints.
		joins = append(joins, fmt.Sprintf("l.%s = r.%s", col.Identifier, col.Identifier))
	}

	// CREATE temporary table which stores keys to load.
	keyCreate = fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s_%d (
			%s
		) ON COMMIT DELETE ROWS
		;`,
		tempTableName,
		binding,
		strings.Join(defs, ", "),
	)

	// INSERT key to load.
	keyInsert = fmt.Sprintf(`
		INSERT INTO %s_%d (
			%s
		) VALUES (
			%s
		);`,
		tempTableName,
		binding,
		strings.Join(keys, ", "),
		strings.Join(keyPH, ", "),
	)

	// SELECT documents included in keys to load.
	keyJoin = fmt.Sprintf(`
		SELECT %d, l.%s
			FROM %s AS l
			JOIN %s_%d AS r
			ON %s
		`,
		binding,
		table.GetColumn(fields.Document).Identifier,
		table.Identifier,
		tempTableName,
		binding,
		strings.Join(joins, " AND "),
	)

	return
}

const tempTableName = "flow_load_key_tmp"
