package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/protocols/flow"
)

// NewSQLiteDriver creates a new Driver for sqlite.
func NewSQLiteDriver() *sqlDriver.Driver {
	return &sqlDriver.Driver{
		NewEndpoint: func(ctx context.Context, et flow.EndpointType, config json.RawMessage) (*sqlDriver.Endpoint, error) {
			var parsed struct {
				Path  string
				Table string
			}

			if err := json.Unmarshal(config, &parsed); err != nil {
				return nil, fmt.Errorf("parsing SQLite configuration: %w", err)
			}
			if parsed.Path == "" {
				return nil, fmt.Errorf("expected SQLite database configuration `path`")
			}
			if parsed.Table == "" {
				return nil, fmt.Errorf("expected SQLite database configuration `table`")
			}

			db, err := sql.Open("sqlite3", parsed.Path)
			if err != nil {
				return nil, fmt.Errorf("opening SQLite database: %w", err)
			}

			var endpoint = &sqlDriver.Endpoint{
				Context:      ctx,
				EndpointType: et,
				DB:           db,
				Generator:    sqlDriver.SQLiteSQLGenerator(),
			}
			endpoint.Tables.Target = parsed.Table
			endpoint.Tables.Checkpoints = sqlDriver.DefaultGazetteCheckpoints
			endpoint.Tables.Specs = sqlDriver.DefaultFlowMaterializations

			return endpoint, nil
		},
		RunTransactions: sqlDriver.RunSQLTransactions,
	}
}
