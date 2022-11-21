package bindings

import (
	"context"
	"database/sql"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestBuildCatalog(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///build.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) error {
		t.Run("config", func(t *testing.T) {
			var out, err = catalog.LoadBuildConfig(db)
			require.NoError(t, err)

			out.Directory = "/stable/path"
			cupaloy.SnapshotT(t, out)
		})
		t.Run("all-collections", func(t *testing.T) {
			var out, err = catalog.LoadAllCollections(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("one-collection", func(t *testing.T) {
			var out, err = catalog.LoadCollection(db, "a/collection")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("one-capture", func(t *testing.T) {
			var out, err = catalog.LoadCapture(db, "example/capture")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("one-derivation", func(t *testing.T) {
			var out, err = catalog.LoadDerivation(db, "a/derivation")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("one-materialization", func(t *testing.T) {
			var out, err = catalog.LoadMaterialization(db, "example/materialization")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("all-captures", func(t *testing.T) {
			var out, err = catalog.LoadAllCaptures(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("all-derivations", func(t *testing.T) {
			var out, err = catalog.LoadAllDerivations(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("all-materializations", func(t *testing.T) {
			var out, err = catalog.LoadAllMaterializations(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("inferences", func(t *testing.T) {
			var out, err = catalog.LoadAllInferences(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("bundle", func(t *testing.T) {
			var out, err = catalog.LoadSchemaBundle(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("all-tests", func(t *testing.T) {
			var out, err = catalog.LoadAllTests(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})

		return nil
	}))
}

func TestBuildSchema(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///b.schema.yaml",
			SourceType: pf.ContentType_JSON_SCHEMA,
		}}
	require.NoError(t, BuildCatalog(args))

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) error {
		t.Run("config", func(t *testing.T) {
			var out, err = catalog.LoadBuildConfig(db)
			require.NoError(t, err)

			out.Directory = "/stable/path"
			cupaloy.SnapshotT(t, out)
		})
		t.Run("inferences", func(t *testing.T) {
			var out, err = catalog.LoadAllInferences(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		t.Run("bundle", func(t *testing.T) {
			var out, err = catalog.LoadSchemaBundle(db)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, out)
		})
		return nil
	}))
}

func TestCatalogSchema(t *testing.T) {
	var schema = CatalogJSONSchema()
	require.True(t, len(schema) > 100)
}
