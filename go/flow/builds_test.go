package flow

import (
	"context"
	"database/sql"
	"runtime"
	"testing"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestBuildReferenceCounting(t *testing.T) {
	var builds, err = NewBuildService("file:///not/used")
	require.NoError(t, err)

	var b1 = builds.Open("an-id")
	require.Equal(t, 1, b1.references)
	var b2 = builds.Open("an-id")
	require.Equal(t, 2, b2.references)
	var b3 = builds.Open("other-id")
	require.Equal(t, 1, b3.references)

	require.NoError(t, b1.Close())
	require.Equal(t, 1, b2.references)

	// We can't test finalizer panics, because there's no way to catch them.
	// Uncomment to see them in action:
	// b2, b3 = nil, nil
	// runtime.GC()

	require.NoError(t, b2.Close())
	require.NoError(t, b3.Close())

	runtime.GC() // Expect no panics.
}

func TestBuildLazyInitAndReuse(t *testing.T) {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "a-build-id",
			Directory:  t.TempDir(),
			Source:     "file:///specs_test.flow.yaml",
			SourceType: pf.ContentType_CATALOG_SPEC,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var builds, err = NewBuildService("file://" + args.Directory)
	require.NoError(t, err)

	// Open. Expect DB is not initialized until first use.
	var b1 = builds.Open(args.BuildId)
	defer b1.Close()

	require.Nil(t, b1.db)

	// Load a collection fixture from the database.
	var collection *pf.CollectionSpec
	require.NoError(t, b1.Extract(func(db *sql.DB) (err error) {
		collection, err = catalog.LoadCollection(db, "example/collection")
		return err
	}))
	require.Equal(t, "example/collection", collection.Collection.String())

	// Database was initialized.
	var db1 = b1.db
	require.NotNil(t, db1)

	var b2 = builds.Open(args.BuildId)
	defer b2.Close()

	// Future calls to Extract of another opened *Build share the same database.
	require.NoError(t, b2.Extract(func(db2 *sql.DB) error {
		require.Equal(t, db1, db2)
		return nil
	}))

	// Similarly, a built SchemaIndex is shared.
	index2, err := b2.SchemaIndex()
	require.NoError(t, err)
	index1, err := b1.SchemaIndex()
	require.NoError(t, err)
	require.Equal(t, index1, index2)

	// Our fixture doesn't build a typescript package, so initialization
	// fails with an error. Expect the error is shared.
	_, err = b1.TypeScriptClient()
	require.Error(t, err)
	require.Equal(t, err, b2.tsErr)

	require.NoError(t, b1.Close())
	require.NoError(t, b2.Close())
}

func TestInitOfMissingBuild(t *testing.T) {
	var builds, err = NewBuildService("file:///dev/null")
	require.NoError(t, err)

	var b1 = builds.Open("a-build")
	defer b1.Close()
	var b2 = builds.Open("a-build")
	defer b2.Close()

	err = b1.Extract(func(db *sql.DB) error {
		panic("not called")
	})
	require.EqualError(t, err,
		"loading build config from /dev/null/a-build: query(\"SELECT build_config FROM meta\"): unable to open database file: not a directory")
	require.Equal(t, err, b2.dbErr)

	require.NoError(t, b1.Close())
	require.NoError(t, b2.Close())
}
