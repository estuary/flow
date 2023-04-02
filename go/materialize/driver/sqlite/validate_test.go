package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sqlDriver "github.com/estuary/flow/go/protocols/materialize/sql"
	"github.com/stretchr/testify/require"
)

func TestValidations(t *testing.T) {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///validate.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var collections []*pf.CollectionSpec
	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		collections, err = catalog.LoadAllCollections(db)
		return err
	}))

	for _, spec := range collections {
		if strings.HasPrefix(spec.Name.String(), "ops") {
			continue
		}
		t.Run(
			fmt.Sprintf("NewSQLProjections-%s", path.Base(spec.Name.String())),
			func(t *testing.T) {
				constraints := sqlDriver.ValidateNewSQLProjections(spec, false)
				cupaloy.SnapshotT(t, constraints)
			})
	}
	t.Run("MatchesExisting", func(t *testing.T) {
		for _, c := range collections {
			if c.Name == "weird-types/optionals" {
				testMatchesExisting(t, c)
				return
			}
		}
		panic("not found")
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
	stringProjection.Inference.Exists = pf.Inference_MUST

	var constraints = sqlDriver.ValidateMatchesExisting(&existingSpec, &proposed)
	var req = []string{"theKey", "string", "bool", "flow_document"}
	for _, field := range req {
		var constraint, ok = constraints[field]
		require.True(t, ok, "constraint must be present for field '%s'", field)
		require.Equal(t, pm.Response_Validated_Constraint_FIELD_REQUIRED, constraint.Type)
	}
	var intConstraint, ok = constraints["int"]
	require.True(t, ok, "missing constraint for 'int' field")
	require.Equal(t, pm.Response_Validated_Constraint_UNSATISFIABLE, intConstraint.Type)

	numConstraint, ok := constraints["number"]
	require.True(t, ok, "missing constraint for 'number' field")
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, numConstraint.Type)

	var proposedSpec = pf.MaterializationSpec_Binding{
		Collection:     proposed,
		FieldSelection: *existingFields,
	}
	var constraintsError = sqlDriver.ValidateSelectedFields(constraints, &proposedSpec)
	require.Error(t, constraintsError)
}
