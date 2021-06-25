package sql

import (
	"fmt"
	"path"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/bindings"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestValidations(t *testing.T) {
	var built, err = bindings.BuildCatalog(bindings.BuildArgs{
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///flow.yaml",
			SourceType:  pf.ContentType_CATALOG_SPEC,
			CatalogPath: filepath.Join(t.TempDir(), "catalog.db"),
		}})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	for _, spec := range built.Collections {
		t.Run(
			fmt.Sprintf("NewSQLProjections-%s", path.Base(spec.Collection.String())),
			func(t *testing.T) {
				var constraints = ValidateNewSQLProjections(&spec)
				cupaloy.SnapshotT(t, constraints)
			})
	}

	t.Run("MatchesExisting", func(t *testing.T) {
		// Test body wants "weird-types/optionals", which orders as 1 alphabetically.
		testMatchesExisting(t, &built.Collections[1])
	})
}

func testMatchesExisting(t *testing.T, collection *pf.CollectionSpec) {
	var existingFields = &pf.FieldSelection{
		Keys:     []string{"theKey"},
		Values:   []string{"string", "bool", "int"},
		Document: "flow_document",
	}
	var existingSpec = pf.MaterializationSpec_Binding{
		Collection:     *collection,
		FieldSelection: *existingFields,
	}

	// Deep copy the collection, which we'll modify and use as the proposal.
	var proposed pf.CollectionSpec
	{
		b, _ := collection.Marshal()
		require.NoError(t, proposed.Unmarshal(b))
	}

	// int projection is changing type to "string", which should result in unsatisfiable
	// constraint
	var intProjection = proposed.GetProjection("int")
	intProjection.Inference.Types = []string{"string"}
	// string projection is going from optional to required, which should be allowed
	var stringProjection = proposed.GetProjection("string")
	stringProjection.Inference.MustExist = true

	var constraints = ValidateMatchesExisting(&existingSpec, &proposed)
	var req = []string{"theKey", "string", "bool", "flow_document"}
	for _, field := range req {
		var constraint, ok = constraints[field]
		require.True(t, ok, "constraint must be present for field '%s'", field)
		require.Equal(t, pm.Constraint_FIELD_REQUIRED, constraint.Type)
	}
	var intConstraint, ok = constraints["int"]
	require.True(t, ok, "missing constraint for 'int' field")
	require.Equal(t, pm.Constraint_UNSATISFIABLE, intConstraint.Type)

	numConstraint, ok := constraints["number"]
	require.True(t, ok, "missing constraint for 'number' field")
	require.Equal(t, pm.Constraint_FIELD_FORBIDDEN, numConstraint.Type)

	var proposedSpec = pf.MaterializationSpec_Binding{
		Collection:     proposed,
		FieldSelection: *existingFields,
	}
	var constraintsError = ValidateSelectedFields(constraints, &proposedSpec)
	require.Error(t, constraintsError)
}
