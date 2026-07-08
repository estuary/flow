package materialize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateBindingFoldedFieldConsistency(t *testing.T) {
	constraint := func(typ Response_Validated_Constraint_Type, folded string) *Response_Validated_Constraint {
		return &Response_Validated_Constraint{Type: typ, FoldedField: folded}
	}
	pc := func(field string, c *Response_Validated_Constraint) *Response_Validated_ProjectionConstraint {
		return &Response_Validated_ProjectionConstraint{Field: field, Constraint: c}
	}

	// Agreement on a non-empty folded_field is valid.
	t.Run("matching folded fields", func(t *testing.T) {
		b := &Response_Validated_Binding{
			ResourcePath: []string{"table"},
			ProjectionConstraints: []*Response_Validated_ProjectionConstraint{
				pc("my_field", constraint(Response_Validated_Constraint_FIELD_OPTIONAL, "MY_FIELD")),
				pc("my_field", constraint(Response_Validated_Constraint_INCOMPATIBLE, "MY_FIELD")),
			},
		}
		require.NoError(t, b.Validate())
	})

	// Empty folded_field (uses the field name itself) is consistent across entries.
	t.Run("empty folded fields agree", func(t *testing.T) {
		b := &Response_Validated_Binding{
			ResourcePath: []string{"table"},
			ProjectionConstraints: []*Response_Validated_ProjectionConstraint{
				pc("my_field", constraint(Response_Validated_Constraint_LOCATION_REQUIRED, "")),
				pc("my_field", constraint(Response_Validated_Constraint_INCOMPATIBLE, "")),
			},
		}
		require.NoError(t, b.Validate())
	})

	// Mismatched folded_field values across entries for the same field must be rejected.
	t.Run("mismatched folded fields", func(t *testing.T) {
		b := &Response_Validated_Binding{
			ResourcePath: []string{"table"},
			ProjectionConstraints: []*Response_Validated_ProjectionConstraint{
				pc("my_field", constraint(Response_Validated_Constraint_FIELD_OPTIONAL, "MY_FIELD")),
				pc("my_field", constraint(Response_Validated_Constraint_INCOMPATIBLE, "my_field_alias")),
			},
		}
		err := b.Validate()
		require.ErrorContains(t, err, "folded_field mismatch")
		require.ErrorContains(t, err, "ProjectionConstraints[1]")
	})
}
