package materialize

import (
	"testing"

	//pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestFieldSelectionAllFields(t *testing.T) {
	var fields = FieldSelection{
		Keys:     []string{"key1", "key2"},
		Values:   []string{"val1", "val2"},
		Document: "flow_document",
	}
	var actual = fields.AllFields()
	var expected = []string{
		"key1",
		"key2",
		"val1",
		"val2",
		"flow_document",
	}
	require.Equal(t, expected, actual)
}

func TestFieldSelectionEqual(t *testing.T) {
	var a = FieldSelection{
		Keys:     []string{"key1", "key2"},
		Values:   []string{"val1", "val2"},
		Document: "flow_document",
	}
	var b = FieldSelection{
		Keys:     []string{"key2", "key1"},
		Values:   []string{"val2", "val1"},
		Document: "root_document",
	}

	require.False(t, a.Equal(&b))

	b.Keys = []string{"key1", "key2"}
	require.False(t, a.Equal(&b))
	require.False(t, b.Equal(&a))
	b.Values = []string{"val1", "val2"}
	require.False(t, a.Equal(&b))
	require.False(t, b.Equal(&a))
	b.Document = "flow_document"
	require.True(t, a.Equal(&b))
	require.True(t, b.Equal(&a))
}
