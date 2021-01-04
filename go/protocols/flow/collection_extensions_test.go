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
			Inference: &inference,
		}

		var result = projection.IsSingleScalarType()
		require.True(t, result, "expected projection to be a single scalar type %v", projection)
	}

}
