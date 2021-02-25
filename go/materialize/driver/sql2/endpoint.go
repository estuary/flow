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
	Context      context.Context
	EndpointType pf.EndpointType
	DB           *sql.DB
	Generator    Generator
	Tables       struct {
		Target      string
		Checkpoints string
		Specs       string
	}
}

// LoadSpec loads the MaterializationSpec for this Endpoint. If |mustExist|,
// an error is returned if the spec doesn't exist. Otherwise, a query error
// is logged and mapped to a nil result (e.x., because the table doesn't exist
// or doesn't yet contain the specified row).
func (e *Endpoint) LoadSpec(mustExist bool) (*pf.MaterializationSpec, error) {
	// Surface connection issues regardless of |mustExist|.
	if err := e.DB.PingContext(e.Context); err != nil {
		return nil, fmt.Errorf("connecting to DB: %w", err)
	}

	var specB64 string
	var spec = new(pf.MaterializationSpec)

	var err = e.DB.QueryRowContext(
		e.Context,
		fmt.Sprintf(
			"SELECT spec FROM %s WHERE table_name=%s;",
			e.Tables.Specs,
			e.Generator.Placeholder(0),
		),
		e.Tables.Target,
	).Scan(&specB64)

	if err != nil && !mustExist {
		log.WithFields(log.Fields{
			"table": e.Tables.Specs,
			"err":   err,
		}).Warn("failed to query materialization spec (the table may not be initialized?)")

		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("querying materialization spec: %w", err)
	} else if specBytes, err := base64.StdEncoding.DecodeString(specB64); err != nil {
		return nil, fmt.Errorf("base64.Decode: %w", err)
	} else if err = spec.Unmarshal(specBytes); err != nil {
		return nil, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err = spec.Validate(); err != nil {
		return nil, fmt.Errorf("validating spec: %w", err)
	}

	return spec, nil
}

// ApplyStatements to the database in a single transaction.
func (e *Endpoint) ApplyStatements(statements []string) error {
	var txn, err = e.DB.BeginTx(e.Context, nil)
	if err != nil {
		return fmt.Errorf("DB.BeginTx: %w", err)
	}

	for i, stmt := range statements {
		if _, err := txn.Exec(stmt); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("executing statement %d: %w", i, err)
		}
	}
	return txn.Commit()
}
