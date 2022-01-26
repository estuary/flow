package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"

	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// StdEndpoint is the *database/sql.DB standard implementation of an endpoint.
type StdEndpoint struct {
	// Parsed configuration of this Endpoint, as a driver-specific type.
	config interface{}
	// Endpoint opened as driver/sql DB.
	db *sql.DB
	// Generator of SQL for this endpoint.
	generator Generator
	// FlowTables
	flowTables FlowTables
}

// NewStdEndpoint composes a new StdEndpoint suitable for sql.DB compatible databases.
func NewStdEndpoint(config interface{}, db *sql.DB, generator Generator, flowTables FlowTables) *StdEndpoint {
	return &StdEndpoint{
		config:     config,
		db:         db,
		generator:  generator,
		flowTables: flowTables,
	}
}

// Config returns the endpoint's config value.
func (e *StdEndpoint) Config() interface{} {
	return e.config
}

// DB returns the embedded *sql.DB.
func (e *StdEndpoint) DB() *sql.DB {
	return e.db
}

// Generator returns the SQL generator.
func (e *StdEndpoint) Generator() *Generator {
	return &e.generator
}

// FlowTables returns the Flow Tables configurations.
func (e *StdEndpoint) FlowTables() *FlowTables {
	return &e.flowTables
}

// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
func (e *StdEndpoint) LoadSpec(ctx context.Context, materialization pf.Materialization) (version string, _ *pf.MaterializationSpec, _ error) {

	// Fail-fast: surface a connection issue.
	if err := e.db.PingContext(ctx); err != nil {
		return "", nil, fmt.Errorf("connecting to DB: %w", err)
	}

	var specB64 string
	var spec = new(pf.MaterializationSpec)

	var err = e.db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"SELECT version, spec FROM %s WHERE materialization=%s;",
			e.flowTables.Specs.Identifier,
			e.generator.Placeholder(0),
		),
		materialization.String(),
	).Scan(&version, &specB64)

	if err != nil {
		log.WithFields(log.Fields{
			"table": e.flowTables.Specs.Identifier,
			"err":   err,
		}).Info("failed to query materialization spec (the table may not be initialized?)")
		return "", nil, nil
	} else if specBytes, err := base64.StdEncoding.DecodeString(specB64); err != nil {
		return version, nil, fmt.Errorf("base64.Decode: %w", err)
	} else if err = spec.Unmarshal(specBytes); err != nil {
		return version, nil, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err = spec.Validate(); err != nil {
		return version, nil, fmt.Errorf("validating spec: %w", err)
	}

	return version, spec, nil
}

// ExecuteStatements executes all of the statements provided in a single transaction.
func (e *StdEndpoint) ExecuteStatements(ctx context.Context, statements []string) error {

	log.Debug("starting transaction")
	var txn, err = e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}
	for i, statement := range statements {
		log.WithField("sql", statement).Debug("executing statement")
		if _, err := txn.Exec(statement); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("executing statement %d: %w", i, err)
		}
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	log.Debug("committed transaction")
	return nil

}

// CreateTableStatement generates a CREATE TABLE statement for the given table. The returned
// statement must not contain any parameter placeholders.
func (e *StdEndpoint) CreateTableStatement(table *Table) (string, error) {
	var builder strings.Builder

	if len(table.Comment) > 0 {
		_, _ = e.generator.CommentRenderer.Write(&builder, table.Comment, "")
	}

	builder.WriteString("CREATE ")
	if table.Temporary {
		builder.WriteString("TEMPORARY ")
	}
	builder.WriteString("TABLE ")
	if table.IfNotExists {
		builder.WriteString("IF NOT EXISTS ")
	}
	builder.WriteString(table.Identifier)
	builder.WriteString(" (\n\t")

	for i, column := range table.Columns {
		if i > 0 {
			builder.WriteString(",\n\t")
		}
		if len(column.Comment) > 0 {
			_, _ = e.generator.CommentRenderer.Write(&builder, column.Comment, "\t")
			// The comment will always end with a newline, but we'll need to add the indentation
			// for the next line. If there's no comment, then the indentation will already be there.
			builder.WriteRune('\t')
		}
		builder.WriteString(column.Identifier)
		builder.WriteRune(' ')

		var resolved, err = e.generator.TypeMappings.GetColumnType(&column)
		if err != nil {
			return "", err
		}
		builder.WriteString(resolved.SQLType)
	}

	builder.WriteString(",\n\n\tPRIMARY KEY(")
	var firstPk = true
	for _, column := range table.Columns {
		if column.PrimaryKey {
			if !firstPk {
				builder.WriteString(", ")
			}
			firstPk = false
			builder.WriteString(column.Identifier)
		}
	}
	// Close the primary key paren, then newline and close the create table statement.
	builder.WriteString(")\n)")
	if table.Temporary && table.TempOnCommit != "" {
		builder.WriteString(" ON COMMIT ")
		builder.WriteString(table.TempOnCommit)
	}
	builder.WriteRune(';')
	return builder.String(), nil
}
