package sql

import (
	"context"

	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
)

// Endpoint is an sql compatible endpoint that allows dialect specific tasks and generators.
type Endpoint interface {

	// LoadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
	LoadSpec(ctx context.Context, materialization pf.Materialization) (string, *pf.MaterializationSpec, error)

	// CreateTableStatement returns the SQL statement to create the specified table in the correct dialect.
	CreateTableStatement(table *Table) (string, error)

	// ExecuteStatements takes a slice of SQL statements and executes them as a single transaction
	// (or as multiple transactions if it's not possible for the implementation) and rolls back
	// if there is a failure.
	ExecuteStatements(ctx context.Context, statements []string) error

	// NewFence installs and returns a new endpoint specific Fence implementation. On return, all
	// older endpoints with matching materialization name and overlapping key-range will be
	// blocked from further database operations. This prevents rogue endpoints from committing
	// further transactions.
	NewFence(ctx context.Context, materialization pf.Materialization, keyBegin, keyEnd uint32) (Fence, error)

	// Generator returns the dialect specific SQL generator for the endpoint.
	Generator() *Generator

	// FlowTables returns the FlowTables definitions for this endpoint.
	FlowTables() *FlowTables
}

// Fence is an installed barrier in a shared checkpoints table which prevents
// other sessions from committing transactions under the fenced ID --
// and prevents this Fence from committing where another session has in turn
// fenced this instance off.
type Fence interface {
	// Fetch the current checkpoint.
	Checkpoint() []byte
	// SetCheckpoint sets the current checkpoint.
	SetCheckpoint(checkpoint []byte)
	// LogEntry returns a logger Entry with context of the current fence to differentiate
	// concurrent threads in the logs.
	LogEntry() *logrus.Entry
}
