package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// Endpoint is a parsed and connected endpoint configuration
type Endpoint struct {
	// Parsed configuration of this Endpoint, as a driver-specific type.
	Config interface{}
	// Context which scopes this Endpoint, e.x. of a gRPC request.
	Context context.Context
	// Endpoint opened as driver/sql DB.
	DB *sql.DB
	// Generator of SQL for this endpoint.
	Generator Generator
	Tables    struct {
		Checkpoints *Table // Table of Flow checkpoints.
		Specs       *Table // Table of MaterializationSpecs.
	}
}

// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
func (e *Endpoint) LoadSpec(materialization pf.Materialization) (version string, _ *pf.MaterializationSpec, _ error) {
	// Fail-fast: surface a connection issue.
	if err := e.DB.PingContext(e.Context); err != nil {
		return "", nil, fmt.Errorf("connecting to DB: %w", err)
	}

	var specB64 string
	var spec = new(pf.MaterializationSpec)

	var err = e.DB.QueryRowContext(
		e.Context,
		fmt.Sprintf(
			"SELECT version, spec FROM %s WHERE materialization=%s;",
			e.Tables.Specs.Identifier,
			e.Generator.Placeholder(0),
		),
		materialization.String(),
	).Scan(&version, &specB64)

	if err != nil {
		log.WithFields(log.Fields{
			"table": e.Tables.Specs.Identifier,
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

// ApplyStatements to the database in a single transaction.
func (e *Endpoint) ApplyStatements(statements []string) error {
	var txn, err = e.DB.BeginTx(e.Context, nil)
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}

	for i, stmt := range statements {
		log.WithField("sql", stmt).Debug("executing statement")
		if _, err := txn.Exec(stmt); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("executing statement %d: %w", i, err)
		}
	}
	return txn.Commit()
}
