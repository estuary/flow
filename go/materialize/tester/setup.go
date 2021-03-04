package tester

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestSetup(t *testing.T, fixture *Fixture) {
	validateResp, err := fixture.Validate()
	require.NoError(t, err, "Validate returned error: %v", err)

	var requireConstraintAllowed = func(field, fieldType string) {
		var constraint = validateResp.Constraints[field]
		if constraint == nil {
			t.Errorf("Validate response is missing constraint for %s field: '%s'", fieldType, field)
		} else if constraint.Type.IsForbidden() {
			t.Errorf("Validate response disallowed selected %s field: '%s', constraint: %v", fieldType, field, constraint)
		}
	}

	// Ensure all the selected projections are allowed
	for _, field := range fixture.Materialization.FieldSelection.Keys {
		requireConstraintAllowed(field, "key")
	}
	for _, field := range fixture.Materialization.FieldSelection.Values {
		requireConstraintAllowed(field, "value")
	}
	requireConstraintAllowed(fixture.Materialization.FieldSelection.Document, "flow document")

	// Ensure there's no extra constraints that aren't part of the collection.
	for field, constraint := range validateResp.Constraints {
		var projection = fixture.Materialization.Collection.GetProjection(field)
		if projection == nil {
			t.Errorf("unexpected validation constraint for field '%s': %v", field, constraint)
		}
	}

	applyResp, err := fixture.Apply(false)
	require.NoError(t, err, "Apply rpc failed")
	log.WithField("actionDescription", applyResp.ActionDescription).Debug("Apply successful")
}
