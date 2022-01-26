package bindings

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestBuildingSchemaIndex(t *testing.T) {
	var bundle = pf.SchemaBundle{
		Bundle: map[string]string{"http://example": "true"},
	}
	var index, err = NewSchemaIndex(&bundle)
	require.NoError(t, err)
	require.False(t, index.indexMemPtr == 0)
}
