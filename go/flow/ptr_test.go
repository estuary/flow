package flow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is a copy of the equivalent test from src/doc/ptr.rs.
func TestPointerCreate(t *testing.T) {
	var cases = []struct {
		ptr   string
		value interface{}
	}{
		{"/foo/2/a", "hello"},
		// Add property to existing object.
		{"/foo/2/b", 3},
		{"/foo/0", false},   // Update existing Null.
		{"/bar", nil},       // Add property to doc root.
		{"/foo/0", true},    // Update from 'false'.
		{"/foo/-", "world"}, // NextIndex extends Array.
		// Index token is interpreted as property because object exists.
		{"/foo/2/4", 5},
		// NextIndex token is also interpreted as property.
		{"/foo/2/-", false},
	}

	var doc interface{}
	for _, tc := range cases {
		var ptr, err = NewPointer(tc.ptr)
		require.NoError(t, err)
		v, err := ptr.Create(&doc)
		require.NoError(t, err)

		*v = tc.value
	}

	var b, err = json.Marshal(doc)
	require.NoError(t, err)

	require.Equal(t, `{"bar":null,"foo":[true,null,{"-":false,"4":5,"a":"hello","b":3},"world"]}`, string(b))

}
