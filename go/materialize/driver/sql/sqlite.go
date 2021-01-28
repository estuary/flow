package sql

import (
	"database/sql"

	pm "github.com/estuary/flow/go/protocols/materialize"
	_ "github.com/mattn/go-sqlite3"
)

// EndpointTypeSQLite is the name of the endpoint type for sqlite, used in the catalog spec.
const EndpointTypeSQLite = "sqlite"

// NewSQLiteDriver creates a new DriverServer for sqlite.
func NewSQLiteDriver() pm.DriverServer {
	var sqlGen = SQLiteSQLGenerator()
	var connectionMan = &StandardSQLConnectionBuilder{
		DriverName: "sqlite3",
		SQLGen:     &sqlGen,
		TxOptions:  sql.TxOptions{},
	}
	return &GenericDriver{
		EndpointType: EndpointTypeSQLite,
		SQLGen:       &sqlGen,
		Connections:  NewCache(connectionMan),
		SQLCache:     make(map[string]*CachedSQL),
	}
}
