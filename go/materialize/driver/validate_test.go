package driver

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/flow"

	//pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestValidations(t *testing.T) {
	var tempdir, err = ioutil.TempDir("", "validate-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempdir)

	cat, err := flow.NewCatalog("../../../catalog.db", tempdir)
	require.NoError(t, err)

	var collections = []string{
		"optionals",
		"required-nullable",
		"optional-multi-types",
	}
	for _, name := range collections {
		var collectionName = fmt.Sprintf("weird-types/%s", name)
		t.Run(fmt.Sprintf("NewSQLProjections-%s", name), func(t *testing.T) {
			collection, err := cat.LoadCollection(collectionName)
			require.NoError(t, err)

			var constraints = ValidateNewSQLProjections(&collection)
			cupaloy.SnapshotT(t, constraints)
		})
	}

	t.Run("MatchesExisting", func(t *testing.T) {
		testMatchesExisting(t, cat)
	})
}

func testMatchesExisting(t *testing.T, catalog *flow.Catalog) {
	existingCollection, err := catalog.LoadCollection("weird-types/optionals")
	require.NoError(t, err)
	var existingFields = pm.FieldSelection{
		Keys:     []string{"theKey"},
		Values:   []string{"string", "bool", "int"},
		Document: "flow_document",
	}
	var existingSpec = MaterializationSpec{
		Collection: existingCollection,
		Fields:     existingFields,
	}

	// Load a new copy of the same collection, which we'll modify and use as the "proposed"
	proposedCollection, err := catalog.LoadCollection("weird-types/optionals")
	require.NoError(t, err)
	// int projection is changing type to "string", which should result in unsatisfiable
	// constraint
	var intProjection = proposedCollection.GetProjection("int")
	intProjection.Inference.Types = []string{"string"}
	// string projection is going from optional to requried, which should be allowed
	var stringProjection = proposedCollection.GetProjection("string")
	stringProjection.Inference.MustExist = true

	var constraints = ValidateMatchesExisting(&existingSpec, &proposedCollection)
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

	var proposedSpec = MaterializationSpec{
		Collection: proposedCollection,
		Fields:     existingFields,
	}
	var constraintsError = ValidateSelectedFields(constraints, &proposedSpec)
	require.Error(t, constraintsError)
}
