package flow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointReduceCases(t *testing.T) {
	// Case: non-patch reduce of empty checkpoint.
	lhs := ConnectorState{}
	require.NoError(t, lhs.Reduce(ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1"}`),
		MergePatch:  false,
	}))
	assertCPEqual(t, ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1"}`),
		MergePatch:  false,
	}, lhs)

	// Case: non-patch reduce of non-empty checkpoint.
	lhs = ConnectorState{
		UpdatedJson: []byte(`{"other":"value"}`),
		MergePatch:  true,
	}
	require.NoError(t, lhs.Reduce(ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1"}`),
		MergePatch:  false,
	}))
	assertCPEqual(t, ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1"}`),
		MergePatch:  false,
	}, lhs)

	// Case: patch reduce of empty checkpoint.
	lhs = ConnectorState{}
	require.NoError(t, lhs.Reduce(ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1","n":null}`),
		MergePatch:  true,
	}))
	assertCPEqual(t, ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1"}`),
		MergePatch:  false,
	}, lhs)

	// Case: patch reduce of non-empty and non-patch checkpoint.
	lhs = ConnectorState{
		UpdatedJson: []byte(`{"other":"value"}`),
		MergePatch:  false,
	}
	require.NoError(t, lhs.Reduce(ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1","n":null}`),
		MergePatch:  true,
	}))
	assertCPEqual(t, ConnectorState{
		UpdatedJson: []byte(`{"other":"value","k1":"v1"}`),
		MergePatch:  false,
	}, lhs)

	// Case: patch reduce of patch checkpoint.
	lhs = ConnectorState{
		UpdatedJson: []byte(`{"other":"value"}`),
		MergePatch:  true,
	}
	require.NoError(t, lhs.Reduce(ConnectorState{
		UpdatedJson: []byte(`{"k1":"v1","n":null}`),
		MergePatch:  true,
	}))
	assertCPEqual(t, ConnectorState{
		UpdatedJson: []byte(`{"other":"value","k1":"v1","n":null}`),
		MergePatch:  true,
	}, lhs)
}

func assertCPEqual(t *testing.T, expected, actual ConnectorState) {
	require.Equalf(t, expected.MergePatch, actual.MergePatch, "expected: %+v, actual: %+v", expected, actual)
	// Unmarshal the json so that the comparisson will not be sensitive to the order of keys
	var expectedJson, actualJson map[string]interface{}
	require.NoError(t, json.Unmarshal(expected.UpdatedJson, &expectedJson))
	require.NoError(t, json.Unmarshal(actual.UpdatedJson, &actualJson))
	require.Equalf(t, expectedJson, actualJson, "expected: %+v, actual: %+v", expected, actual)
}
