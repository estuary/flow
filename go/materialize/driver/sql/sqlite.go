package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	pm "github.com/estuary/flow/go/protocols/materialize"
)

// NewSQLiteDriver creates a new DriverServer for sqlite.
func NewSQLiteDriver() pm.DriverServer {
	var sqlGen = SQLiteSQLGenerator()
	var connectionMan = &StandardSQLConnectionBuilder{
		DriverName: "sqlite3",
		SQLGen:     &sqlGen,
		TxOptions:  sql.TxOptions{},
	}
	var parseConfig = func(config json.RawMessage) (uri string, table string, err error) {
		var parsed struct {
			Path  string
			Table string
		}
		if err = json.Unmarshal(config, &parsed); err != nil {
			err = fmt.Errorf("parsing SQLite configuration: %w", err)
			return
		}
		if parsed.Path == "" {
			err = fmt.Errorf("expected SQLite database configuration `path`")
			return
		}
		if parsed.Table == "" {
			err = fmt.Errorf("expected SQLite database configuration `table`")
			return
		}
		return parsed.Path, parsed.Table, nil
	}

	return &GenericDriver{
		ParseConfig: parseConfig,
		SQLGen:      &sqlGen,
		Connections: NewCache(connectionMan),
		SQLCache:    make(map[string]*CachedSQL),
	}
}
