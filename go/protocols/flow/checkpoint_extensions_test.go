package flow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointReduceCases(t *testing.T) {
	// Case: non-patch reduce of empty checkpoint.
	lhs := DriverCheckpoint{}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}))
	assertCPEqual(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}, lhs)

	// Case: non-patch reduce of non-empty checkpoint.
	lhs = DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"other":"value"}`),
		Rfc7396MergePatch:    true,
	}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}))
	assertCPEqual(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}, lhs)

	// Case: patch reduce of empty checkpoint.
	lhs = DriverCheckpoint{}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1","n":null}`),
		Rfc7396MergePatch:    true,
	}))
	assertCPEqual(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}, lhs)

	// Case: patch reduce of non-empty and non-patch checkpoint.
	lhs = DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"other":"value"}`),
		Rfc7396MergePatch:    false,
	}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1","n":null}`),
		Rfc7396MergePatch:    true,
	}))
	assertCPEqual(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"other":"value","k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}, lhs)

	// Case: patch reduce of patch checkpoint.
	lhs = DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"other":"value"}`),
		Rfc7396MergePatch:    true,
	}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1","n":null}`),
		Rfc7396MergePatch:    true,
	}))
	assertCPEqual(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"other":"value","k1":"v1","n":null}`),
		Rfc7396MergePatch:    true,
	}, lhs)
}

func assertCPEqual(t *testing.T, expected, actual DriverCheckpoint) {
	require.Equalf(t, expected.Rfc7396MergePatch, actual.Rfc7396MergePatch, "expected: %+v, actual: %+v", expected, actual)
	// Unmarshal the json so that the comparisson will not be sensitive to the order of keys
	var expectedJson, actualJson map[string]interface{}
	require.NoError(t, json.Unmarshal(expected.DriverCheckpointJson, &expectedJson))
	require.NoError(t, json.Unmarshal(actual.DriverCheckpointJson, &actualJson))
	require.Equalf(t, expectedJson, actualJson, "expected: %+v, actual: %+v", expected, actual)
}
