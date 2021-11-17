package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointReduceCases(t *testing.T) {
	// Case: non-patch reduce of empty checkpoint.
	var lhs = DriverCheckpoint{}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}))
	require.Equal(t, DriverCheckpoint{
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
	require.Equal(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1"}`),
		Rfc7396MergePatch:    false,
	}, lhs)

	// Case: patch reduce of empty checkpoint.
	lhs = DriverCheckpoint{}
	require.NoError(t, lhs.Reduce(DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"k1":"v1","n":null}`),
		Rfc7396MergePatch:    true,
	}))
	require.Equal(t, DriverCheckpoint{
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
	require.Equal(t, DriverCheckpoint{
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
	require.Equal(t, DriverCheckpoint{
		DriverCheckpointJson: []byte(`{"other":"value","k1":"v1","n":null}`),
		Rfc7396MergePatch:    true,
	}, lhs)
}
