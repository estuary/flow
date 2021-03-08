package test

import (
	"testing"

	"github.com/estuary/flow/go/materialize/driver/postgres"
	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	"github.com/estuary/flow/go/materialize/tester"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var gen = sqlDriver.PostgresSQLGenerator()
	var spec = tester.NewMaterialization(pf.EndpointType_POSTGRESQL, "")
	var table = sqlDriver.TableForMaterialization("test_table", "", spec)

	var keyCreate, keyJoin, err = postgres.BuildSQL(&gen, table, spec.FieldSelection)
	require.NoError(t, err)

	require.Equal(t, `
		CREATE TEMPORARY TABLE flow_load_key_tmp (
			"key1" BIGINT NOT NULL, "key2" TEXT NOT NULL
		) ON COMMIT DELETE ROWS
		;`, keyCreate)

	require.Equal(t, `
		SELECT l."flow_document"
			FROM "test_table" AS l
			JOIN flow_load_key_tmp AS r
			ON l."key1" = r."key1" AND l."key2" = r."key2"
		;`, keyJoin)
}
