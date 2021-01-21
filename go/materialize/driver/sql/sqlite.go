package sql

import (
	"database/sql"

	pm "github.com/estuary/flow/go/protocols/materialize"
)

// NewSQLiteDriver creates a new DriverServer for sqlite.
func NewSQLiteDriver() pm.DriverServer {
	var sqlGen = SQLiteSQLGenerator()
	var connectionMan = &StandardSQLConnectionManager{
		DriverName: "sqlite3",
		SQLGen:     &sqlGen,
		TxOptions:  sql.TxOptions{},
	}
	return &GenericDriver{
		EndpointType: "sqlite",
		SQLGen:       &sqlGen,
		Connections:  NewCache(connectionMan),
		SQLCache:     make(map[string]*CachedSQL),
	}
}
