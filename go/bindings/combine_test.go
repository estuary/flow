package bindings

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	_ "github.com/mattn/go-sqlite3" // Import for registration side-effect.
	"github.com/stretchr/testify/require"
)

func TestCombineBindings(t *testing.T) {
	const dbPath = "../../catalog.db"
	const collection = "testing/int-strings"

	var db, err = sql.Open("sqlite3", "file:"+dbPath+"?immutable=true&mode=ro")
	require.NoError(t, err)

	var schemaURI string
	var row = db.QueryRow("SELECT schema_uri FROM collections "+
		"WHERE collection_name = ?", collection)
	require.NoError(t, row.Scan(&schemaURI))

	builder, err := NewCombineBuilder(dbPath)
	require.NoError(t, err)

	combiner, err := builder.Open(
		schemaURI,
		[]string{"/i"},
		[]string{"/s/1", "/i"},
		"/meta/_uuid",
		false)
	require.NoError(t, err)

	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 32, "s": ["one"]}`)))
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 42, "s": ["two"]}`)))
	require.NoError(t, combiner.Flush())
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 42, "s": ["three"]}`)))
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 32, "s": ["four"]}`)))

	// Expect duplicate calls aren't a problem.
	require.NoError(t, combiner.CloseSend())
	require.NoError(t, combiner.CloseSend())

	expectCombineFixture(t, combiner.Finish)

	builder.Release(combiner)
}

type callback = func(raw json.RawMessage, key []byte, fields tuple.Tuple) error

func expectCombineFixture(t *testing.T, finish func(callback) error) {
	var expect = []struct {
		i int64
		s []string
	}{
		{32, []string{"one", "four"}},
		{42, []string{"two", "three"}},
	}

	require.NoError(t, finish(func(raw json.RawMessage, key []byte, fields tuple.Tuple) error {
		t.Log("doc", string(raw), "fields", fields)

		var doc struct {
			I    int64
			S    []string
			Meta struct {
				UUID string `json:"_uuid"`
			}
		}

		require.NoError(t, json.Unmarshal(raw, &doc))
		require.Equal(t, expect[0].i, doc.I)
		require.Equal(t, expect[0].s, doc.S)
		require.Equal(t, string(pf.DocumentUUIDPlaceholder), doc.Meta.UUID)

		require.Equal(t, tuple.Tuple{doc.I}.Pack(), key)
		require.Equal(t, tuple.Tuple{doc.S[1], doc.I}, fields)

		expect = expect[1:]
		return nil
	}))
}
