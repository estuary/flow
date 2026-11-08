package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// `priority` was widened from `uint32` to `int32` so that a binding can be
// de-prioritized with respect to the default priority of zero. `int32` (not
// `sint32`) keeps the encoding of non-negative values byte-identical to the
// prior `uint32` encoding, so a new reader understands an old writer's spec
// and vice versa. Negative values encode as the ten-byte, sign-extended
// varint that protobuf specifies for `int32`.
func TestBindingPriorityRoundTrip(t *testing.T) {
	for _, priority := range []int32{-2147483648, -100, -1, 0, 1, 40, 2147483647} {
		var encoded, err = (&MaterializationSpec_Binding{Priority: priority}).Marshal()
		require.NoError(t, err)

		var recovered MaterializationSpec_Binding
		require.NoError(t, recovered.Unmarshal(encoded))
		require.Equal(t, priority, recovered.Priority)
	}

	// A non-negative priority encodes exactly as `uint32` did: field 9's varint
	// tag, followed by the plain varint of the value. Other binding fields are
	// non-nullable messages which always encode, so compare against a binding
	// which differs only in its priority.
	var withPriority, err = (&MaterializationSpec_Binding{Priority: 40}).Marshal()
	require.NoError(t, err)

	var withoutPriority []byte
	withoutPriority, err = (&MaterializationSpec_Binding{}).Marshal()
	require.NoError(t, err)

	require.Equal(t, append(withoutPriority, 0x48, 0x28), withPriority)
}
