package bindings

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// Validation failures are expected to be quite common, so we should pay special attention to how
// they're shown to the user.
func TestValidationFailuresAreLogged(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///int-strings.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	var collection *pf.CollectionSpec

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) (err error) {
		if collection, err = catalog.LoadCollection(db, "int-strings"); err != nil {
			return fmt.Errorf("loading collection: %w", err)
		}
		return nil
	}))

	var opsLogs = make(chan ops.Log)

	combiner, err := NewCombine(newChanPublisher(opsLogs, pf.LogLevel_warn))
	require.NoError(t, err)
	defer combiner.Destroy()

	err = combiner.Configure(
		collection.Collection.String(),
		collection.Collection,
		collection.WriteSchemaJson,
		collection.UuidPtr,
		collection.KeyPtrs,
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": "not an int"}`)))

	_, err = combiner.Drain(func(_ bool, raw json.RawMessage, packedKey, packedFields []byte) error {
		require.Fail(t, "expected combine callback not to be called")
		return fmt.Errorf("not a real error")
	})
	require.Error(t, err)

	var opsLog = <-opsLogs
	cupaloy.SnapshotT(t, err, opsLog.Level, opsLog.Message, string(opsLog.Fields))
}

func TestCombineBindings(t *testing.T) {
	var args = BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			Directory:  t.TempDir(),
			Source:     "file:///int-strings.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, BuildCatalog(args))

	var collection *pf.CollectionSpec

	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) (err error) {
		if collection, err = catalog.LoadCollection(db, "int-strings"); err != nil {
			return fmt.Errorf("loading collection: %w", err)
		}
		return nil
	}))

	combiner, err := NewCombine(localPublisher)
	require.NoError(t, err)

	// Loop to exercise re-use of a Combiner.
	for i := 0; i != 5; i++ {

		// Re-configure the Combiner every other iteration.
		if i%2 == 0 {
			err := combiner.Configure(
				collection.Collection.String(),
				collection.Collection,
				collection.WriteSchemaJson,
				collection.UuidPtr,
				collection.KeyPtrs,
				[]string{"/s/1", "/i"},
			)
			require.NoError(t, err)
		}

		require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 32, "s": ["one"]}`)))
		require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 42, "s": ["three"]}`)))
		require.NoError(t, pollExpectNoOutput(combiner.svc))
		require.NoError(t, combiner.ReduceLeft(json.RawMessage(`{"i": 42, "s": ["two"]}`)))
		require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 32, "s": ["four"]}`)))

		expectCombineFixture(t, combiner.Drain)
	}
}

func expectCombineCallback(t *testing.T) CombineCallback {
	var expect = []struct {
		i int64
		s []string
	}{
		{32, []string{"one", "four"}},
		{42, []string{"two", "three"}},
	}

	return func(_ bool, raw json.RawMessage, packedKey, packedFields []byte) error {
		t.Log("doc", string(raw))

		var doc struct {
			I    int64
			S    []string
			Meta struct {
				UUID string
			} `json:"_meta"`
		}

		require.NoError(t, json.Unmarshal(raw, &doc))
		require.Equal(t, expect[0].i, doc.I)
		require.Equal(t, expect[0].s, doc.S)
		require.Equal(t, string(pf.DocumentUUIDPlaceholder), doc.Meta.UUID)

		require.Equal(t, tuple.Tuple{doc.I}.Pack(), packedKey)
		require.Equal(t, tuple.Tuple{doc.S[1], doc.I}.Pack(), packedFields)

		expect = expect[1:]
		return nil
	}
}

func expectCombineFixture(t *testing.T, finish func(CombineCallback) (*pf.CombineAPI_Stats, error)) {
	var stats, err = finish(expectCombineCallback(t))
	require.NoError(t, err)
	t.Log(stats)
	// Technically, we already test the correctness of stats on the rust side, but these assertions
	// exist to ensure that the stats successfully make it back into the Go side correctly.
	require.Equal(t, uint32(1), stats.Left.Docs)
	require.Equal(t, uint32(23), stats.Left.Bytes)
	require.Equal(t, uint32(3), stats.Right.Docs)
	require.Equal(t, uint32(72), stats.Right.Bytes)
	require.Equal(t, uint32(2), stats.Out.Docs)
	require.Equal(t, uint32(167), stats.Out.Bytes)
}
