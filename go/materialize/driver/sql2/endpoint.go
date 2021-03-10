package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// Endpoint is a parsed and connected endpoint configuration
type Endpoint struct {
	// Parsed configuration of this Endpoint, as a driver-specific type.
	Config interface{}
	// Context which scopes this Endpoint, e.x. of a gRPC request.
	Context context.Context
	// Catalog name of this endpoint.
	Name string
	// Endpoint opened as driver/sql DB.
	DB *sql.DB
	// Should we materialize document delta updates (as opposed to fully reduced
	// documents)? If "true", we issue no Loads and do not cache Stored
	// documents of prior transactions, so that each Store reflects combined
	// document deltas of the current transaction only.
	DeltaUpdates bool
	// Fully qualified name of the table, as '.'-separated components.
	// TODO(johnny): Unify with Tables.TargetName.
	TablePath []string
	// Generator of SQL for this endpoint.
	Generator Generator
	Tables    struct {
		TargetName  string // Table to which we materialize.
		Checkpoints *Table // Table of Gazette checkpoints.
		Specs       *Table // Table of MaterializationSpecs.
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
			e.Tables.Specs.Identifier,
			e.Generator.Placeholder(0),
		),
		e.Tables.TargetName,
	).Scan(&specB64)

	if err != nil && !mustExist {
		log.WithFields(log.Fields{
			"table": e.Tables.Specs.Identifier,
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
	} else if e.Name != spec.EndpointName {
		return nil, fmt.Errorf("cannot change endpoint name of an active materialization (from %v to %v)",
			spec.EndpointName, e.Name)
	} else if !reflect.DeepEqual(e.TablePath, spec.EndpointResourcePath) {
		return nil, fmt.Errorf("persisted table path %v is inconsistent with this endpoint's path of %v",
			spec.EndpointResourcePath, e.TablePath)
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
