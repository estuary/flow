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
			MustExist: true,
			Types:     []string{scalar},
		}
		var maybeUndefined = Inference{
			MustExist: false,
			Types:     []string{scalar},
		}
		var maybeNull = Inference{
			MustExist: true,
			Types:     []string{scalar, "null"},
		}
		var maybeBoth = Inference{
			MustExist: false,
			Types:     []string{scalar, "null"},
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
			MustExist: true,
			Types:     []string{"string", "int", "null"},
		},
		{
			MustExist: true,
			// This is just documenting the behavior. We could definitely allow this in the future,
			// but we already collapse these into a single type during catalog builds, so we're not
			// likely to ever see this at runtime.
			Types: []string{"int", "number"},
		},
		{
			MustExist: true,
			Types:     []string{"boolean", "int"},
		},
		{
			MustExist: true,
			Types:     []string{},
		},
		{
			MustExist: true,
			Types:     []string{"null"},
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
