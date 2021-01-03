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
