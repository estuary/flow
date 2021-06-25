package postgres_test

import (
	"path/filepath"
	"testing"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/materialize/driver/postgres"
	sqlDriver "github.com/estuary/flow/go/materialize/driver/sql2"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///flow.yaml",
			SourceType:  pf.ContentType_CATALOG_SPEC,
			CatalogPath: filepath.Join(t.TempDir(), "catalog.db"),
		},
		MaterializeDriverFn: materialize.NewDriver,
	})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	var gen = sqlDriver.PostgresSQLGenerator()
	var spec = &built.Materializations[0]
	var table = sqlDriver.TableForMaterialization("test_table", "", &gen.IdentifierQuotes, spec.Bindings[0])

	keyCreate, keyInsert, keyJoin, err := postgres.BuildSQL(&gen, 123, table, spec.Bindings[0].FieldSelection)
	require.NoError(t, err)

	require.Equal(t, `
		CREATE TEMPORARY TABLE flow_load_key_tmp_123 (
			key1 BIGINT NOT NULL, key2 BOOLEAN NOT NULL
		) ON COMMIT DELETE ROWS
		;`, keyCreate)

	require.Equal(t, `
		INSERT INTO flow_load_key_tmp_123 (
			key1, key2
		) VALUES (
			$1, $2
		);`, keyInsert)

	// Note the intentional missing semicolon, as this is a subquery.
	require.Equal(t, `
		SELECT 123, l.flow_document
			FROM test_table AS l
			JOIN flow_load_key_tmp_123 AS r
			ON l.key1 = r.key1 AND l.key2 = r.key2
		`, keyJoin)
}
