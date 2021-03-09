package test

import (
	"testing"

	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/materialize/driver/sqlite"
	"github.com/estuary/flow/go/materialize/tester"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var gen = sqlDriver.SQLiteSQLGenerator()
	var spec = tester.NewMaterialization(pf.EndpointType_POSTGRESQL, "")
	var table = sqlDriver.TableForMaterialization("test_table", "", &gen.IdentifierQuotes, spec)

	var attach, keyCreate, keyInsert, keyJoin, keyTruncate, err = sqlite.BuildSQL(&gen, table, spec.FieldSelection)
	require.NoError(t, err)

	require.Equal(t, `ATTACH DATABASE '' AS load ;`, attach)

	require.Equal(t, `
		CREATE TABLE load.keys (
			key1 INTEGER NOT NULL, key2 TEXT NOT NULL
		);`, keyCreate)

	require.Equal(t, `
		INSERT INTO load.keys (
			key1, key2
		) VALUES (
			?, ?
		);`, keyInsert)

	require.Equal(t, `
		SELECT l.flow_document
			FROM test_table AS l
			JOIN load.keys AS r
			ON l.key1 = r.key1 AND l.key2 = r.key2
		;`, keyJoin)

	require.Equal(t, `DELETE FROM load.keys ;`, keyTruncate)
}
