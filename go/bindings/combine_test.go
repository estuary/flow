package bindings

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	"github.com/stretchr/testify/require"
)

func TestCombineBindings(t *testing.T) {
	built, err := BuildCatalog(BuildArgs{
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///int-strings.flow.yaml",
			CatalogPath: filepath.Join(t.TempDir(), "catalog.db"),
		}})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	var collection = built.Collections[1]
	schemaIndex, err := NewSchemaIndex(&built.Schemas)
	require.NoError(t, err)

	combiner, err := NewCombine(
		schemaIndex,
		collection.SchemaUri,
		collection.KeyPtrs,
		[]string{"/s/1", "/i"},
		collection.UuidPtr,
	)
	require.NoError(t, err)
	defer combiner.Destroy()

	// Loop to exercise re-use of a Combiner.
	for i := 0; i != 5; i++ {
		require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 32, "s": ["one"]}`)))
		require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 42, "s": ["three"]}`)))
		require.NoError(t, pollExpectNoOutput(combiner.svc))
		require.NoError(t, combiner.ReduceLeft(json.RawMessage(`{"i": 42, "s": ["two"]}`)))
		require.NoError(t, combiner.CombineRight(json.RawMessage(`{"i": 32, "s": ["four"]}`)))

		if i%2 == 1 {
			// PrepareToDrain may optionally be called ahead of Drain.
			require.NoError(t, combiner.PrepareToDrain())
		}

		expectCombineFixture(t, combiner.Drain)
	}
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
