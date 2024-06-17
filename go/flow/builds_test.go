package flow

import (
	"context"
	"database/sql"
	"net/url"
	"path"
	"runtime"
	"testing"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestBuildBaseParsing(t *testing.T) {
	var builds, err = NewBuildService("https://example/")
	require.NoError(t, err)
	require.Equal(t, &url.URL{Scheme: "https", Host: "example", Path: "/"}, builds.baseURL)

	builds, err = NewBuildService("https://example/with/slash/")
	require.NoError(t, err)
	require.Equal(t, &url.URL{Scheme: "https", Host: "example", Path: "/with/slash/"}, builds.baseURL)

	_, err = NewBuildService("https://example/no/slash")
	require.EqualError(t, err, "base URL \"https://example/no/slash\" must end in '/'")
	_, err = NewBuildService("https://example")
	require.EqualError(t, err, "base URL \"https://example\" must end in '/'")
}

func TestBuildReferenceCounting(t *testing.T) {
	var builds, err = NewBuildService("file:///not/used/")
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
	var dir = t.TempDir()
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "6666666666666666",
			BuildDb:    path.Join(dir, "6666666666666666"),
			Source:     "file:///specs_test.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var builds, err = NewBuildService("file://" + dir + "/")
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
	require.Equal(t, "example/collection", collection.Name.String())

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

	// Close both builds, dropping the reference count to zero.
	require.NoError(t, b1.Close())
	require.NoError(t, b2.Close())

	// Open the database again, raising the count from zero -> one.
	var b3 = builds.Open(args.BuildId)
	defer b3.Close()

	// Ensure we can load the collection.
	require.NoError(t, b3.Extract(func(db *sql.DB) (err error) {
		collection, err = catalog.LoadCollection(db, "example/collection")
		return err
	}))
	require.Equal(t, "example/collection", collection.Name.String())
}

func TestInitOfMissingBuild(t *testing.T) {
	var builds, err = NewBuildService("file:///dev/null/")
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
