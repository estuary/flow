package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	_ "github.com/mattn/go-sqlite3" // Import for register side-effects.
	log "github.com/sirupsen/logrus"
)

// NewSQLiteDriver creates a new Driver for sqlite.
func NewSQLiteDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		NewEndpoint: func(ctx context.Context, name string, config json.RawMessage) (*sqlDriver.Endpoint, error) {
			var parsed = new(struct {
				Path  string
				Table string
			})

			if err := json.Unmarshal(config, parsed); err != nil {
				return nil, fmt.Errorf("parsing SQLite configuration: %w", err)
			}
			if parsed.Path == "" {
				return nil, fmt.Errorf("expected SQLite database configuration `path`")
			}
			if parsed.Table == "" {
				return nil, fmt.Errorf("expected SQLite database configuration `table`")
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
				"path":  parsed.Path,
				"table": parsed.Table,
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

			var endpoint = &sqlDriver.Endpoint{
				Config:       parsed,
				Context:      ctx,
				Name:         name,
				DB:           db,
				DeltaUpdates: false, // TODO: supporting deltas requires relaxing CREATE TABLE generation.
				TablePath:    []string{parsed.Table},
				Generator:    sqlDriver.SQLiteSQLGenerator(),
			}
			endpoint.Tables.Checkpoints = sqlDriver.GazetteCheckpointsTable(sqlDriver.DefaultGazetteCheckpoints)
			endpoint.Tables.Specs = sqlDriver.FlowMaterializationsTable(sqlDriver.DefaultFlowMaterializations)

			return endpoint, nil
		},
		NewTransactor: func(ep *sqlDriver.Endpoint, spec *pf.MaterializationSpec, fence *sqlDriver.Fence) (lifecycle.Transactor, error) {
			var err error
			var target = sqlDriver.TableForMaterialization(ep.TargetName(), "", &ep.Generator.IdentifierQuotes, spec)
			var d = &transactor{ctx: ep.Context}

			// Build all SQL statements and parameter converters.
			var attachSQL, keyCreateSQL string
			attachSQL, keyCreateSQL, d.load.insert.sql, d.load.query.sql, d.load.truncate.sql, err =
				BuildSQL(&ep.Generator, target, spec.FieldSelection)
			if err != nil {
				return nil, fmt.Errorf("building SQL: %w", err)
			}

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

			if d.load.conn, err = ep.DB.Conn(d.ctx); err != nil {
				return nil, fmt.Errorf("load DB.Conn: %w", err)
			}
			if d.store.conn, err = ep.DB.Conn(d.ctx); err != nil {
				return nil, fmt.Errorf("store DB.Conn: %w", err)
			}

			// Execute one-time, session scoped statements.
			for _, sql := range []string{attachSQL, keyCreateSQL} {
				if _, err = d.load.conn.ExecContext(d.ctx, sql); err != nil {
					return nil, fmt.Errorf("Exec(%s): %w", sql, err)
				}
			}
			// Prepare query statements.
			for _, t := range []struct {
				conn *sql.Conn
				sql  string
				stmt **sql.Stmt
			}{
				{d.load.conn, d.load.insert.sql, &d.load.insert.stmt},
				{d.load.conn, d.load.query.sql, &d.load.query.stmt},
				{d.load.conn, d.load.truncate.sql, &d.load.truncate.stmt},
				{d.store.conn, d.store.insert.sql, &d.store.insert.stmt},
				{d.store.conn, d.store.update.sql, &d.store.update.stmt},
			} {
				*t.stmt, err = t.conn.PrepareContext(d.ctx, t.sql)
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
		conn   *sql.Conn
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
		conn   *sql.Conn
		fence  *sqlDriver.Fence
		txn    *sql.Tx
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

func (d *transactor) Load(it *lifecycle.LoadIterator, _ <-chan struct{}, loaded func(json.RawMessage) error) error {
	if _, err := d.load.truncate.stmt.Exec(); err != nil {
		return fmt.Errorf("truncating Loads: %w", err)
	}

	for it.Next() {
		if converted, err := d.load.params.Convert(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if _, err = d.load.insert.stmt.Exec(converted...); err != nil {
			return fmt.Errorf("inserting Load key: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Issue a join of the target table and (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.query.stmt.Query()
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var document sql.RawBytes

		if err = rows.Scan(&document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	return nil
}

func (d *transactor) Prepare(prepare *pm.TransactionRequest_Prepare) (*pm.TransactionResponse_Prepared, error) {
	d.store.fence.Checkpoint = prepare.FlowCheckpoint

	return &pm.TransactionResponse_Prepared{
		DriverCheckpointJson: nil, // Not used.
	}, nil
}

func (d *transactor) Store(it *lifecycle.StoreIterator) error {
	var err error

	if d.store.txn, err = d.store.conn.BeginTx(d.ctx, nil); err != nil {
		return fmt.Errorf("conn.BeginTx: %w", err)
	}

	var insertStmt = d.store.txn.Stmt(d.store.insert.stmt)
	var updateStmt = d.store.txn.Stmt(d.store.update.stmt)

	for it.Next() {
		if it.Exists {
			converted, err := d.store.update.params.Convert(
				append(append(it.Values, it.RawJSON), it.Key...))
			if err != nil {
				return fmt.Errorf("converting update parameters: %w", err)
			}
			if _, err = updateStmt.Exec(converted...); err != nil {
				return fmt.Errorf("updating document: %w", err)
			}
		} else {
			converted, err := d.store.insert.params.Convert(
				append(append(it.Key, it.Values...), it.RawJSON))
			if err != nil {
				return fmt.Errorf("converting insert parameters: %w", err)
			}
			if _, err = insertStmt.Exec(converted...); err != nil {
				return fmt.Errorf("inserting document: %w", err)
			}
		}
	}
	return nil
}

func (d *transactor) Commit() error {
	var err error

	if d.store.txn == nil {
		// If Store was skipped, we wont' have begun a DB transaction yet.
		if d.store.txn, err = d.store.conn.BeginTx(d.ctx, nil); err != nil {
			return fmt.Errorf("conn.BeginTx: %w", err)
		}
	}

	if err = d.store.fence.Update(
		func(_ context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error) {
			if result, err := d.store.txn.Exec(sql, arguments...); err != nil {
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
func BuildSQL(gen *sqlDriver.Generator, table *sqlDriver.Table, fields pf.FieldSelection) (
	attach, keyCreate, keyInsert, keyJoin, keyTruncate string, err error) {

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

	// We attach a connection-scoped temporary DB to host our "keys to load" temp table.
	// This is to ensure we can always write to this table, no matter what other locks
	// are held by the main database or other temp tables.
	attach = "ATTACH DATABASE '' AS load ;"

	// A temporary table which stores keys to load.
	keyCreate = fmt.Sprintf(`
		CREATE TABLE load.keys (
			%s
		);`,
		strings.Join(defs, ", "),
	)

	// INSERT key to load.
	keyInsert = fmt.Sprintf(`
		INSERT INTO load.keys (
			%s
		) VALUES (
			%s
		);`,
		strings.Join(keys, ", "),
		strings.Join(keyPH, ", "),
	)

	// SELECT documents included in keys to load.
	keyJoin = fmt.Sprintf(`
		SELECT l.%s
			FROM %s AS l
			JOIN load.keys AS r
			ON %s
		;`,
		table.GetColumn(fields.Document).Identifier,
		table.Identifier,
		strings.Join(joins, " AND "),
	)

	// DELETE keys to load, after query.
	keyTruncate = `DELETE FROM load.keys ;`

	return
}
