package flow

import (
	"context"
	"encoding/json"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestBasicCombineLifecycle(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	spec, err := catalog.LoadDerivedCollection("testing/int-strings")
	require.NoError(t, err)

	wh, err := NewWorkerHost("combine", "--catalog", catalog.LocalPath())
	require.Nil(t, err)
	defer wh.Stop()

	var ctx = context.Background()
	combiner, err := NewCombine(ctx, pf.NewCombineClient(wh.Conn), &spec)
	require.NoError(t, err)

	require.NoError(t, combiner.Open([]string{"/i", "/s/1"}, false))
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 32, "s": ["one"]}`)))
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 42, "s": ["two"]}`)))
	require.NoError(t, combiner.flush())
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 42, "s": ["three"]}`)))
	require.NoError(t, combiner.Add(json.RawMessage(`{"i": 32, "s": ["four"]}`)))

	// Expect duplicate calls aren't a problem.
	require.NoError(t, combiner.CloseSend())
	require.NoError(t, combiner.CloseSend())

	var out []pf.IndexedCombineResponse
	require.NoError(t, combiner.Finish(func(p pf.IndexedCombineResponse) error {
		out = append(out, p)
		return nil
	}))

	for i, expect := range []struct {
		i uint64
		s []string
	}{
		{32, []string{"one", "four"}},
		{42, []string{"two", "three"}},
	} {
		var actual = out[i]

		var unpacked struct {
			I uint64
			S []string
		}
		require.NoError(t, json.Unmarshal(
			actual.Arena.Bytes(actual.DocsJson[actual.Index]),
			&unpacked,
		))
		require.Equal(t, expect.i, unpacked.I)
		require.Equal(t, expect.s, unpacked.S)

		// Expect "/i" was extracted from the combined document.
		require.Equal(t, pf.Field_Value{
			Kind:     pf.Field_Value_UNSIGNED,
			Unsigned: expect.i,
		}, actual.Fields[0].Values[actual.Index])
		// As was "/s/1".
		require.Equal(t, expect.s[1],
			string(actual.Arena.Bytes(actual.Fields[1].Values[actual.Index].Bytes)))
	}
}
