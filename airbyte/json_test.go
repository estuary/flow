package airbyte

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfiguredCatalogMarshaling(t *testing.T) {
	var start = `{
        "streams": [{
            "stream": {"name": "foo","json_schema":true},
            "sync_mode": "incremental",
            "destination_sync_mode": "append",
            "primary_key": ["/yea", "/boiiii"],
            "projections": {
                "space": "/balls",
                "blazing": "/saddles"
            }
        }],
        "tail": true,
        "range": {
            "begin": "00000000",
            "end": "FFFFFFFF"
        }
    }`
	var resultOne = ConfiguredCatalog{}
	require.NoError(t, json.Unmarshal([]byte(start), &resultOne))

	// We always serialize using the namespaced fields.
	var serJson, err = json.Marshal(&resultOne)
	require.NoError(t, err)
	require.Contains(t, string(serJson), `"estuary.dev/projections":`)
	require.Contains(t, string(serJson), `"estuary.dev/tail":`)
	require.Contains(t, string(serJson), `"estuary.dev/range":`)

	// Deserialize again and assert that we get the same struct value as the first result.
	var roundTripped = ConfiguredCatalog{}
	require.NoError(t, json.Unmarshal(serJson, &roundTripped))
	require.Equal(t, resultOne, roundTripped)
}
