package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSingleScalarType(t *testing.T) {
	var scalars = []string{"string", "boolean", "integer", "number"}

	var truthy []Inference
	for _, scalar := range scalars {
		var basic = Inference{
			Types: []string{scalar},
		}
		var maybeUndefined = Inference{
			Types: []string{scalar},
		}
		var maybeNull = Inference{
			Types: []string{scalar, "null"},
		}
		var maybeBoth = Inference{
			Types: []string{scalar, "null"},
		}
		truthy = append(truthy, basic, maybeUndefined, maybeNull, maybeBoth)
	}
	for _, inference := range truthy {
		var projection = Projection{
			Field:     "canary",
			Ptr:       "/foo/bar",
			Inference: inference,
		}

		var result = projection.Inference.IsSingleScalarType()
		require.True(t, result, "expected projection to be a single scalar type %v", projection)
	}

	var falsey = []Inference{
		{
			Types: []string{"string", "int", "null"},
		},
		{
			// This is just documenting the behavior. We could definitely allow this in the future,
			// but we already collapse these into a single type during catalog builds, so we're not
			// likely to ever see this at runtime.
			Types: []string{"int", "number"},
		},
		{
			Types: []string{"boolean", "int"},
		},
		{
			Types: []string{},
		},
		{
			Types: []string{"null"},
		},
	}
	for _, inference := range falsey {
		var projection = Projection{
			Field:     "canary",
			Ptr:       "/foo/bar",
			Inference: inference,
		}
		var result = projection.Inference.IsSingleScalarType()
		require.False(t, result, "expected projection to not be a single scalar type: %v", projection)
	}

}

func TestIsRootLevelProjection(t *testing.T) {
	testCases := []struct {
		name     string
		ptr      string
		expected bool
	}{
		{
			name:     "root document projection",
			ptr:      "",
			expected: false,
		},
		{
			name:     "root level field",
			ptr:      "/field",
			expected: true,
		},
		{
			name:     "root level field with special chars",
			ptr:      "/user-name",
			expected: true,
		},
		{
			name:     "nested field",
			ptr:      "/nested/field",
			expected: false,
		},
		{
			name:     "deeply nested field",
			ptr:      "/deeply/nested/field",
			expected: false,
		},
		{
			name:     "array index",
			ptr:      "/array/0",
			expected: false,
		},
		{
			name:     "root level array",
			ptr:      "/array",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			projection := Projection{
				Field: "test_field",
				Ptr:   tc.ptr,
			}
			result := projection.IsRootLevelProjection()
			require.Equal(t, tc.expected, result, "IsRootLevelProjection() for ptr %q", tc.ptr)
		})
	}
}
