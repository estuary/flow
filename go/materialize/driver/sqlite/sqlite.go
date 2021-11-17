package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	sqlDriver "github.com/estuary/protocols/materialize/sql"
	_ "github.com/mattn/go-sqlite3" // Import for register side-effects.
	log "github.com/sirupsen/logrus"
)

// config represents the endpoint configuration for sqlite.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Path string `json:"path"`
}

// Validate the configuration.
func (c config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("expected SQLite database configuration `path`")
	}
	return nil
}

type tableConfig struct {
	Table string `json:"table"`
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected SQLite database configuration `table`")
	}
	return nil
}

func (c tableConfig) Path() sqlDriver.ResourcePath {
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return false // SQLite doesn't support delta updates.
}

// NewSQLiteDriver creates a new Driver for sqlite.
func NewSQLiteDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		DocumentationURL: "https://docs.estuary.dev/#FIXME",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewResource:      func(sqlDriver.Endpoint) sqlDriver.Resource { return new(tableConfig) },
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (sqlDriver.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing SQLite configuration: %w", err)
			}

			if strings.HasPrefix(parsed.Path, ":memory:") {
				// Directly pass to SQLite.
			} else if u, err := url.Parse(parsed.Path); err != nil {
				return nil, fmt.Errorf("parsing path %q: %w", parsed.Path, err)
			} else if !u.IsAbs() {
				return nil, fmt.Errorf("path %q is not absolute", parsed.Path)
			} else if u.Scheme == "file" {
				// We can directly pass file:// schemes to SQLite.
			} else {
				// Path is absolute and non-local (e.x. https://some/database.db).
				// Mangle to turn into a file opened relative to the current directory.
				var parts = append([]string{u.Host}, strings.Split(u.Path, "/")...)
				parsed.Path = strings.Join(parts, "_")

				if u.RawQuery != "" {
					parsed.Path += "?" + u.RawQuery
				}
			}

			log.WithFields(log.Fields{
				"path": parsed.Path,
			}).Info("opening database")

			// SQLite / go-sqlite3 is a bit fickle about raced opens of a newly created database,
			// often returning "database is locked" errors. We can resolve by ensuring one sql.Open
			// completes before the next starts. This is only required for SQLite, not other drivers.
			sqliteOpenMu.Lock()
			db, err := sql.Open("sqlite3", parsed.Path)
			if err == nil {
				err = db.PingContext(ctx)
			}
			sqliteOpenMu.Unlock()

			if err != nil {
				return nil, fmt.Errorf("opening SQLite database %q: %w", parsed.Path, err)
			}

			return sqlDriver.NewStdEndpoint(parsed, db, sqlDriver.SQLiteSQLGenerator(), sqlDriver.DefaultFlowTables("")), nil

		},
		NewTransactor: func(
			ctx context.Context,
			epi sqlDriver.Endpoint,
			spec *pf.MaterializationSpec,
			fence sqlDriver.Fence,
			resources []sqlDriver.Resource,
		) (_ pm.Transactor, err error) {
			var ep = epi.(*sqlDriver.StdEndpoint)
			var d = &transactor{
				gen: ep.Generator(),
			}
			d.store.fence = fence.(*sqlDriver.StdFence)

			// Establish connections.
			if d.load.conn, err = ep.DB().Conn(ctx); err != nil {
				return nil, fmt.Errorf("load DB.Conn: %w", err)
			}
			if d.store.conn, err = ep.DB().Conn(ctx); err != nil {
				return nil, fmt.Errorf("store DB.Conn: %w", err)
			}

			// Attach temporary DB used for staging keys to load.
			if _, err = d.load.conn.ExecContext(ctx, attachSQL); err != nil {
				return nil, fmt.Errorf("Exec(%s): %w", attachSQL, err)
			}

			for _, spec := range spec.Bindings {
				var target = sqlDriver.ResourcePath(spec.ResourcePath).Join()
				if err = d.addBinding(ctx, target, spec); err != nil {
					return nil, fmt.Errorf("%s: %w", target, err)
				}
			}

			// Build a query which unions the results of each load subquery.
			var subqueries []string
			for _, b := range d.bindings {
				subqueries = append(subqueries, b.load.query.sql)
			}
			var loadAllSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

			d.load.stmt, err = d.load.conn.PrepareContext(ctx, loadAllSQL)
			if err != nil {
				return nil, fmt.Errorf("conn.PrepareContext(%s): %w", loadAllSQL, err)
			}

			return d, nil
		},
	}
}

type transactor struct {
	gen *sqlDriver.Generator

	// Variables exclusively used by Load.
	load struct {
		conn *sql.Conn
		stmt *sql.Stmt
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		conn  *sql.Conn
		fence *sqlDriver.StdFence
		txn   *sql.Tx
	}
	bindings []*binding
}

type binding struct {
	// Variables exclusively used by Load.
	load struct {
		params sqlDriver.ParametersConverter
		insert struct {
			sql  string
			stmt *sql.Stmt
		}
		query struct {
			sql  string
			stmt *sql.Stmt
		}
		truncate struct {
			sql  string
			stmt *sql.Stmt
		}
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		insert struct {
			params sqlDriver.ParametersConverter
			sql    string
			stmt   *sql.Stmt
		}
		update struct {
			params sqlDriver.ParametersConverter
			sql    string
			stmt   *sql.Stmt
		}
	}
}

func (t *transactor) addBinding(ctx context.Context, targetName string, spec *pf.MaterializationSpec_Binding) error {
	var err error
	var b = new(binding)
	var target = sqlDriver.TableForMaterialization(targetName, "", t.gen.IdentifierRenderer, spec)

	// Build all SQL statements and parameter converters.
	var keyCreateSQL string
	keyCreateSQL, b.load.insert.sql, b.load.query.sql, b.load.truncate.sql, err =
		BuildSQL(t.gen, len(t.bindings), target, spec.FieldSelection)
	if err != nil {
		return fmt.Errorf("building SQL: %w", err)
	}

	_, b.load.params, err = t.gen.QueryOnPrimaryKey(target, spec.FieldSelection.Document)
	if err != nil {
		return fmt.Errorf("building load SQL: %w", err)
	}
	b.store.insert.sql, b.store.insert.params, err = t.gen.InsertStatement(target)
	if err != nil {
		return fmt.Errorf("building insert SQL: %w", err)
	}
	b.store.update.sql, b.store.update.params, err = t.gen.UpdateStatement(
		target,
		append(append([]string{}, spec.FieldSelection.Values...), spec.FieldSelection.Document),
		spec.FieldSelection.Keys)
	if err != nil {
		return fmt.Errorf("building update SQL: %w", err)
	}

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err = t.load.conn.ExecContext(ctx, keyCreateSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", keyCreateSQL, err)
	}
	// Prepare query statements.
	for _, s := range []struct {
		conn *sql.Conn
		sql  string
		stmt **sql.Stmt
	}{
		{t.load.conn, b.load.insert.sql, &b.load.insert.stmt},
		{t.load.conn, b.load.query.sql, &b.load.query.stmt},
		{t.load.conn, b.load.truncate.sql, &b.load.truncate.stmt},
		{t.store.conn, b.store.insert.sql, &b.store.insert.stmt},
		{t.store.conn, b.store.update.sql, &b.store.update.stmt},
	} {
		*s.stmt, err = s.conn.PrepareContext(ctx, s.sql)
		if err != nil {
			return fmt.Errorf("conn.PrepareContext(%s): %w", s.sql, err)
		}
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func (d *transactor) Load(
	it *pm.LoadIterator,
	// We ignore priorCommitCh and priorAcknowledgedCh because we stage the
	// contents of the iterator, evaluating loads after it's fully drained.
	_, _ <-chan struct{},
	loaded func(int, json.RawMessage) error,
) error {
	// Remove rows left over from the last transaction.
	for _, b := range d.bindings {
		if _, err := b.load.truncate.stmt.Exec(); err != nil {
			return fmt.Errorf("truncating Loads: %w", err)
		}
	}

	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.load.params.Convert(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if _, err = b.load.insert.stmt.Exec(converted...); err != nil {
			return fmt.Errorf("inserting Load key: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.stmt.Query()
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document sql.RawBytes

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	return nil
}

func (d *transactor) Prepare(ctx context.Context, prepare pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	d.store.fence.SetCheckpoint(prepare.FlowCheckpoint)
	return pf.DriverCheckpoint{}, nil
}

func (d *transactor) Store(it *pm.StoreIterator) error {
	var err error

	if d.store.txn, err = d.store.conn.BeginTx(it.Context(), nil); err != nil {
		return fmt.Errorf("conn.BeginTx: %w", err)
	}

	var insertStmts = make([]*sql.Stmt, len(d.bindings))
	var updateStmts = make([]*sql.Stmt, len(d.bindings))

	for i, b := range d.bindings {
		insertStmts[i] = d.store.txn.Stmt(b.store.insert.stmt)
		updateStmts[i] = d.store.txn.Stmt(b.store.update.stmt)
	}

	for it.Next() {
		var b = d.bindings[it.Binding]

		if it.Exists {
			converted, err := b.store.update.params.Convert(
				append(append(it.Values, it.RawJSON), it.Key...))
			if err != nil {
				return fmt.Errorf("converting update parameters: %w", err)
			}
			if _, err = updateStmts[it.Binding].Exec(converted...); err != nil {
				return fmt.Errorf("updating document: %w", err)
			}
		} else {
			converted, err := b.store.insert.params.Convert(
				append(append(it.Key, it.Values...), it.RawJSON))
			if err != nil {
				return fmt.Errorf("converting insert parameters: %w", err)
			}
			if _, err = insertStmts[it.Binding].Exec(converted...); err != nil {
				return fmt.Errorf("inserting document: %w", err)
			}
		}
	}
	return nil
}

func (d *transactor) Commit(ctx context.Context) error {
	var err error

	if d.store.txn == nil {
		// If Store was skipped, we won't have begun a DB transaction yet.
		if d.store.txn, err = d.store.conn.BeginTx(ctx, nil); err != nil {
			return fmt.Errorf("conn.BeginTx: %w", err)
		}
	}

	if err = d.store.fence.Update(ctx,
		func(ctx context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error) {
			if result, err := d.store.txn.ExecContext(ctx, sql, arguments...); err != nil {
				return 0, fmt.Errorf("txn.Exec: %w", err)
			} else if rowsAffected, err = result.RowsAffected(); err != nil {
				return 0, fmt.Errorf("result.RowsAffected: %w", err)
			}
			return
		},
	); err != nil {
		return fmt.Errorf("fence.Update: %w", err)
	}

	if err := d.store.txn.Commit(); err != nil {
		return fmt.Errorf("store.txn.Commit: %w", err)
	}
	d.store.txn = nil

	return nil
}

// Acknowledge is a no-op since the SQLite database is authoritative.
func (d *transactor) Acknowledge(context.Context) error { return nil }

func (d *transactor) Destroy() {
	if d.store.txn != nil {
		d.store.txn.Rollback()
	}
	if err := d.load.conn.Close(); err != nil {
		log.WithField("err", err).Error("failed to close load connection")
	}
	if err := d.store.conn.Close(); err != nil {
		log.WithField("err", err).Error("failed to close store connection")
	}
}

var sqliteOpenMu sync.Mutex

// BuildSQL builds SQL statements used for SQLite materialization.
func BuildSQL(gen *sqlDriver.Generator, binding int, table *sqlDriver.Table, fields pf.FieldSelection) (
	keyCreate, keyInsert, keyJoin, keyTruncate string, err error) {

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

		// JOIN constraint.
		joins = append(joins, fmt.Sprintf("l.%s = r.%s", col.Identifier, col.Identifier))
	}

	// A temporary table which stores keys to load.
	keyCreate = fmt.Sprintf(`
		CREATE TABLE load.keys_%d (
			%s
		);`,
		binding,
		strings.Join(defs, ", "),
	)

	// INSERT key to load.
	keyInsert = fmt.Sprintf(`
		INSERT INTO load.keys_%d (
			%s
		) VALUES (
			%s
		);`,
		binding,
		strings.Join(keys, ", "),
		strings.Join(keyPH, ", "),
	)

	// SELECT documents included in keys to load.
	keyJoin = fmt.Sprintf(`
		SELECT %d, l.%s
			FROM %s AS l
			JOIN load.keys_%d AS r
			ON %s
		`,
		binding,
		table.GetColumn(fields.Document).Identifier,
		table.Identifier,
		binding,
		strings.Join(joins, " AND "),
	)

	// DELETE keys to load, after query.
	keyTruncate = fmt.Sprintf(`DELETE FROM load.keys_%d ;`, binding)

	return
}

// We attach a connection-scoped temporary DB to host our "keys to load" temp tables.
// This is to ensure we can always write to this table, no matter what other locks
// are held by the main database or other temp tables.
const attachSQL = "ATTACH DATABASE '' AS load ;"
