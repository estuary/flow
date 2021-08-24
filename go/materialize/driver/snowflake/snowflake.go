package snowflake

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	sqlDriver "github.com/estuary/protocols/materialize/sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
)

// config represents the endpoint configuration for snowflake.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Account   string `json:"account"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Database  string `json:"database"`
	Schema    string `json:"schema"`
	Warehouse string `json:"warehouse,omitempty"`
	Role      string `json:"role,omitempty"`
	Region    string `json:"region,omitempty"`

	tempdir string
}

func (c config) asSnowflakeConfig() sf.Config {
	return sf.Config{
		Account:   c.Account,
		User:      c.User,
		Password:  c.Password,
		Database:  c.Database,
		Schema:    c.Schema,
		Warehouse: c.Warehouse,
		Role:      c.Role,
		Region:    c.Region,
	}
}

func (c config) Validate() error {
	if c.Account == "" {
		return fmt.Errorf("expected account")
	}
	if c.Database == "" {
		return fmt.Errorf("expected database")
	}
	if c.Schema == "" {
		return fmt.Errorf("expected schema")
	}
	return nil
}

type tableConfig struct {
	base *config

	Table string `json:"table"`
	Delta bool   `json:"delta_updates,omitempty"`
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func (c tableConfig) Path() sqlDriver.ResourcePath {
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

// The Snowflake driver Params map uses string pointers as values, which is what this is used for.
var trueString = "true"

// NewDriver creates a new Driver for Snowflake.
func NewDriver(tempdir string) *sqlDriver.Driver {
	return &sqlDriver.Driver{
		DocumentationURL: "https://docs.estuary.dev/#FIXME",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewResource: func(endpoint *sqlDriver.Endpoint) sqlDriver.Resource {
			return &tableConfig{base: endpoint.Config.(*config)}
		},
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (*sqlDriver.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing Snowflake configuration: %w", err)
			}

			// Build a DSN connection string.
			var configCopy = parsed.asSnowflakeConfig()
			// client_session_keep_alive causes the driver to issue a periodic keepalive request.
			// Without this, the authentication token will expire after 4 hours of inactivity.
			// The Params map will not have been initialized if the endpoint config didn't specify
			// it, so we check and initialize here if needed.
			if configCopy.Params == nil {
				configCopy.Params = make(map[string]*string)
			}
			configCopy.Params["client_session_keep_alive"] = &trueString
			dsn, err := sf.DSN(&configCopy)
			if err != nil {
				return nil, fmt.Errorf("building Snowflake DSN: %w", err)
			}

			log.WithFields(log.Fields{
				"account":   parsed.Account,
				"database":  parsed.Database,
				"role":      parsed.Role,
				"user":      parsed.User,
				"warehouse": parsed.Warehouse,
			}).Info("opening Snowflake")

			db, err := sql.Open("snowflake", dsn)
			if err == nil {
				err = db.PingContext(ctx)
			}

			if err != nil {
				return nil, fmt.Errorf("opening Snowflake database: %w", err)
			}
			parsed.tempdir = tempdir

			var endpoint = &sqlDriver.Endpoint{
				Config:    parsed,
				Context:   ctx,
				DB:        db,
				Generator: SQLGenerator(),
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
		) (_ pm.Transactor, err error) {
			var d = &transactor{
				ctx: ep.Context,
				cfg: ep.Config.(*config),
				gen: &ep.Generator,
			}
			d.store.fence = fence

			// Establish connections.
			if d.load.conn, err = ep.DB.Conn(d.ctx); err != nil {
				return nil, fmt.Errorf("load DB.Conn: %w", err)
			}
			if d.store.conn, err = ep.DB.Conn(d.ctx); err != nil {
				return nil, fmt.Errorf("store DB.Conn: %w", err)
			}

			// Create stage for file-based transfers.
			if _, err = d.load.conn.ExecContext(d.ctx, createStageSQL); err != nil {
				return nil, fmt.Errorf("creating transfer stage : %w", err)
			}

			for _, spec := range spec.Bindings {
				var target = sqlDriver.ResourcePath(spec.ResourcePath).Join()
				if err = d.addBinding(target, spec); err != nil {
					return nil, fmt.Errorf("%s: %w", target, err)
				}
			}

			return d, nil
		},
	}
}

// SQLGenerator returns a SQLGenerator for the Snowflake SQL dialect.
func SQLGenerator() sqlDriver.Generator {
	var variantMapper = sqlDriver.ConstColumnType{
		SQLType: "VARIANT",
		ValueConverter: func(i interface{}) (interface{}, error) {
			switch ii := i.(type) {
			case []byte:
				return json.RawMessage(ii), nil
			case json.RawMessage:
				return ii, nil
			case nil:
				return json.RawMessage(nil), nil
			default:
				return nil, fmt.Errorf("invalid type %#v for variant", i)
			}
		},
	}
	var typeMappings = sqlDriver.ColumnTypeMapper{
		sqlDriver.ARRAY:   variantMapper,
		sqlDriver.BINARY:  sqlDriver.RawConstColumnType("BINARY"),
		sqlDriver.BOOLEAN: sqlDriver.RawConstColumnType("BOOLEAN"),
		sqlDriver.INTEGER: sqlDriver.RawConstColumnType("INTEGER"),
		sqlDriver.NUMBER:  sqlDriver.RawConstColumnType("DOUBLE"),
		sqlDriver.OBJECT:  variantMapper,
		sqlDriver.STRING: sqlDriver.StringTypeMapping{
			Default: sqlDriver.RawConstColumnType("STRING"),
		},
	}
	var nullable sqlDriver.TypeMapper = sqlDriver.NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner:       typeMappings,
	}

	return sqlDriver.Generator{
		CommentConf:      sqlDriver.LineComment(),
		IdentifierQuotes: sqlDriver.DoubleQuotes(),
		Placeholder:      sqlDriver.QuestionMarkPlaceholder,
		TypeMappings:     nullable,
		QuoteStringValue: sqlDriver.DefaultQuoteStringValue,
	}
}

type transactor struct {
	ctx context.Context
	cfg *config
	gen *sqlDriver.Generator

	// Variables exclusively used by Load.
	load struct {
		conn *sql.Conn
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		conn  *sql.Conn
		fence *sqlDriver.Fence
	}
	bindings []*binding
}

type binding struct {
	// Variables exclusively used by Load.
	load struct {
		params  sqlDriver.ParametersConverter
		sql     string
		stage   *scratchFile
		hasKeys bool
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		stage     *scratchFile
		params    sqlDriver.ParametersConverter
		mergeSQL  string
		copySQL   string
		mustMerge bool
		hasDocs   bool
	}
}

func (t *transactor) addBinding(targetName string, spec *pf.MaterializationSpec_Binding) error {
	var d = new(binding)
	var err error
	var target = sqlDriver.TableForMaterialization(targetName, "", &t.gen.IdentifierQuotes, spec)

	// Create local scratch files used for loads and stores.
	if d.load.stage, err = newScratchFile(t.cfg.tempdir); err != nil {
		return fmt.Errorf("newScratchFile: %w", err)
	}
	if d.store.stage, err = newScratchFile(t.cfg.tempdir); err != nil {
		return fmt.Errorf("newScratchFile: %w", err)
	}

	// Build all SQL statements and parameter converters.
	d.load.sql, d.store.copySQL, d.store.mergeSQL = BuildSQL(
		len(t.bindings), target, spec.FieldSelection, d.load.stage.uuid, d.store.stage.uuid)

	_, d.load.params, err = t.gen.QueryOnPrimaryKey(target, spec.FieldSelection.Document)
	if err != nil {
		return fmt.Errorf("building load params: %w", err)
	}
	_, d.store.params, err = t.gen.InsertStatement(target)
	if err != nil {
		return fmt.Errorf("building insert params: %w", err)
	}

	t.bindings = append(t.bindings, d)
	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.load.params.Convert(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = b.load.stage.Encode(converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		}
		b.load.hasKeys = true
	}
	if it.Err() != nil {
		return it.Err()
	}

	var subqueries []string
	// PUT staged keys to Snowflake in preparation for querying.
	for _, b := range d.bindings {
		if !b.load.hasKeys {
			// Pass.
		} else if err := b.load.stage.put(d.cfg); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			subqueries = append(subqueries, b.load.sql)
			b.load.hasKeys = false // Reset for next transaction.
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}
	var loadAllSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	// Issue a join of the target table and (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.conn.QueryContext(d.ctx, loadAllSQL)
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

func (d *transactor) Prepare(prepare *pm.TransactionRequest_Prepare) (_ *pm.TransactionResponse_Prepared, err error) {
	d.store.fence.Checkpoint = prepare.FlowCheckpoint

	return &pm.TransactionResponse_Prepared{
		DriverCheckpointJson: nil, // Not used.
	}, nil
}

func (d *transactor) Store(it *pm.StoreIterator) error {
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.store.params.Convert(
			append(append(it.Key, it.Values...), it.RawJSON),
		); err != nil {
			return fmt.Errorf("converting Store: %w", err)
		} else if err = b.store.stage.Encode(converted); err != nil {
			return fmt.Errorf("encoding Store to scratch file: %w", err)
		}

		if it.Exists {
			b.store.mustMerge = true
		}
		b.store.hasDocs = true
	}
	return nil
}

func (d *transactor) Commit() error {
	for _, b := range d.bindings {
		if b.store.hasDocs {
			// PUT staged keys to Snowflake in preparation for querying.
			if err := b.store.stage.put(d.cfg); err != nil {
				return fmt.Errorf("load.stage(): %w", err)
			}
		}
	}

	// Start a transaction for our Store phase.
	var txn, err = d.store.conn.BeginTx(d.ctx, nil)
	if err != nil {
		return fmt.Errorf("conn.BeginTx: %w", err)
	}
	defer txn.Rollback()

	// Apply the client's prepared checkpoint to our fence.
	if err = d.store.fence.Update(
		func(_ context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error) {
			if result, err := txn.Exec(sql, arguments...); err != nil {
				return 0, fmt.Errorf("txn.Exec: %w", err)
			} else if rowsAffected, err = result.RowsAffected(); err != nil {
				return 0, fmt.Errorf("result.RowsAffected: %w", err)
			}
			return
		},
	); err != nil {
		return fmt.Errorf("fence.Update: %w", err)
	}

	for _, b := range d.bindings {
		if !b.store.hasDocs {
			// No table update required
		} else if !b.store.mustMerge {
			// We can issue a faster COPY INTO the target table.
			if _, err = d.store.conn.ExecContext(d.ctx, b.store.copySQL); err != nil {
				return fmt.Errorf("copying Store documents: %w", err)
			}
		} else {
			// We must MERGE into the target table.
			if _, err = d.store.conn.ExecContext(d.ctx, b.store.mergeSQL); err != nil {
				return fmt.Errorf("merging Store documents: %w", err)
			}
		}

		// Reset for next transaction.
		b.store.hasDocs = false
		b.store.mustMerge = false
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("txn.Commit: %w", err)
	}

	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()

	for _, b := range d.bindings {
		b.load.stage.destroy()
		b.store.stage.destroy()
	}
}

type scratchFile struct {
	uuid uuid.UUID
	file *os.File
	bw   *bufio.Writer
	*json.Encoder
}

func (f *scratchFile) destroy() {
	// TODO remove from Snowflake.
	os.Remove(f.file.Name())
	f.file.Close()
}

func newScratchFile(tempdir string) (*scratchFile, error) {
	var uuid, err = uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	var path = filepath.Join(tempdir, uuid.String())
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating scratch %q: %w", path, err)
	}
	var bw = bufio.NewWriter(file)
	var enc = json.NewEncoder(bw)

	return &scratchFile{
		uuid:    uuid,
		file:    file,
		bw:      bw,
		Encoder: enc,
	}, nil
}

func (f *scratchFile) put(cfg *config) error {
	if err := f.bw.Flush(); err != nil {
		return fmt.Errorf("scratch.Flush: %w", err)
	}

	var query = fmt.Sprintf(
		`PUT file://%s @flow_v1 AUTO_COMPRESS=FALSE SOURCE_COMPRESSION=NONE OVERWRITE=TRUE ;`,
		f.file.Name(),
	)
	var cmd = exec.Command(
		"snowsql",
		fmt.Sprintf("--accountname=%s", cfg.Account),
		fmt.Sprintf("--username=%s", cfg.User),
		fmt.Sprintf("--dbname=%s", cfg.Database),
		fmt.Sprintf("--schemaname=%s", cfg.Schema),
		fmt.Sprintf("--rolename=%s", cfg.Role),
		fmt.Sprintf("--warehouse=%s", cfg.Warehouse),
		fmt.Sprintf("--query=%s", query),
		"--noup", // Don't auto-upgrade.
		"--option=quiet=True",
		"--option=friendly=False",
	)
	cmd.Env = append(os.Environ(), fmt.Sprintf("SNOWSQL_PWD=%s", cfg.Password))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("snowsql failed: %w", err)
	}

	if err := f.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate after stage: %w", err)
	} else if _, err = f.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek after truncate: %w", err)
	}
	f.bw.Reset(f.file)

	return nil
}

// BuildSQL generates SQL used by Snowflake.
func BuildSQL(binding int, table *sqlDriver.Table, fields pf.FieldSelection, loadUUID, storeUUID uuid.UUID) (
	keyJoin, copyInto, mergeInto string) {

	var exStore, names, rValues []string
	for idx, name := range fields.AllFields() {
		var col = table.GetColumn(name)
		exStore = append(exStore, fmt.Sprintf("$1[%d] AS %s", idx, col.Identifier))
		names = append(names, col.Identifier)
		rValues = append(rValues, fmt.Sprintf("r.%s", col.Identifier))
	}
	var exLoad, joins []string
	for idx, name := range fields.Keys {
		var col = table.GetColumn(name)
		exLoad = append(exLoad, fmt.Sprintf("$1[%d] AS %s", idx, col.Identifier))
		joins = append(joins, fmt.Sprintf("%s.%s = r.%s", table.Identifier, col.Identifier, col.Identifier))
	}
	var updates []string
	for _, name := range append(fields.Values, fields.Document) {
		var col = table.GetColumn(name)
		updates = append(updates, fmt.Sprintf("%s.%s = r.%s", table.Identifier, col.Identifier, col.Identifier))
	}

	keyJoin = fmt.Sprintf(`
		SELECT %d, %s.%s
		FROM %s
		JOIN (
			SELECT %s
			FROM @flow_v1/%s
		) AS r
		ON %s
		`,
		binding,
		table.Identifier,
		table.GetColumn(fields.Document).Identifier,
		table.Identifier,
		strings.Join(exLoad, ", "),
		loadUUID.String(),
		strings.Join(joins, " AND "),
	)

	var storeSubquery = fmt.Sprintf(`
			SELECT %s
			FROM @flow_v1/%s
		`,
		strings.Join(exStore, ", "),
		storeUUID.String(),
	)

	copyInto = fmt.Sprintf(`
		COPY INTO %s (
			%s
		) FROM (%s)
		;`,
		table.Identifier,
		strings.Join(names, ", "),
		storeSubquery,
	)

	mergeInto = fmt.Sprintf(`
		MERGE INTO %s
		USING (%s) AS r
		ON %s
		WHEN MATCHED AND IS_NULL_VALUE(r.%s) THEN
			DELETE
		WHEN MATCHED THEN
			UPDATE SET %s
		WHEN NOT MATCHED THEN
			INSERT (%s)
			VALUES (%s)
		;`,
		table.Identifier,
		storeSubquery,
		strings.Join(joins, " AND "),
		table.GetColumn(fields.Document).Identifier,
		strings.Join(updates, ", "),
		strings.Join(names, ", "),
		strings.Join(rValues, ", "),
	)

	return
}

const createStageSQL = `
		CREATE STAGE IF NOT EXISTS flow_v1
			FILE_FORMAT = (
				TYPE = JSON
				BINARY_FORMAT = BASE64
			)
			COMMENT = 'Internal stage used by Estuary Flow to stage loaded & stored documents'
		;`
