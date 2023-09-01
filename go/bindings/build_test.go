package bindings

import (
	"context"
	"database/sql"
	"path"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func TestBuildCatalog(t *testing.T) {
	pb.RegisterGRPCDispatcher("local") // Required (only) by sqlite.InProcessServer.

	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///build.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) error {
		t.Run("config", func(t *testing.T) {
			var out, err = catalog.LoadBuildConfig(db)
			require.NoError(t, err)

			out.BuildDb = "/stable/path/build.db"
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
			var out, err = catalog.LoadCollection(db, "a/derivation")
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
		t.Run("all-materializations", func(t *testing.T) {
			var out, err = catalog.LoadAllMaterializations(db)
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
