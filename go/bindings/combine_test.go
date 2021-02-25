package bindings

import (
	"encoding/json"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	"github.com/stretchr/testify/require"
)

func TestCombineBindings(t *testing.T) {
	var catalog, err = flow.NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	collection, err := catalog.LoadCollection("testing/int-strings")
	require.NoError(t, err)
	bundle, err := catalog.LoadSchemaBundle()
	require.NoError(t, err)
	schemaIndex, err := NewSchemaIndex(bundle)
	require.NoError(t, err)

	var builder = NewCombineBuilder(schemaIndex)

	combiner, err := builder.Open(
		collection.SchemaUri,
		collection.KeyPtrs,
		[]string{"/s/1", "/i"},
		collection.UuidPtr,
	)
	require.NoError(t, err)

	require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 32, "s": ["one"]}`)))
	require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 42, "s": ["three"]}`)))
	require.NoError(t, combiner.Flush())
	require.NoError(t, combiner.ReduceLeft(json.RawMessage(`{"i": 42, "s": ["two"]}`)))
	require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 32, "s": ["four"]}`)))

	// Expect duplicate calls aren't a problem.
	require.NoError(t, combiner.CloseSend())
	require.NoError(t, combiner.CloseSend())

	expectCombineFixture(t, combiner.Finish)

	builder.Release(combiner)
}

func expectCombineFixture(t *testing.T, finish func(CombineCallback) error) {
	var expect = []struct {
		i int64
		s []string
	}{
		{32, []string{"one", "four"}},
		{42, []string{"two", "three"}},
	}

	require.NoError(t, finish(func(_ bool, raw json.RawMessage, packedKey, packedFields []byte) error {
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
	}))
}
