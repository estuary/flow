package flow

import (
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestLoadCollection(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	spec, err := catalog.LoadCollection("testing/int-strings")
	require.NoError(t, err)

	stripPrefix(&spec.SchemaUri)
	cupaloy.SnapshotT(t, spec)
}

func TestLoadCapturedCollections(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)

	specs, err := catalog.LoadCapturedCollections()
	require.NoError(t, err)
	require.NotEmpty(t, specs)

	for i := range specs {
		stripPrefix(&specs[i].SchemaUri)
	}
	cupaloy.SnapshotT(t, specs)
}

func TestLoadDerivedCollection(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	spec, err := catalog.LoadDerivedCollection("testing/int-strings")
	require.NoError(t, err)

	stripPrefix(&spec.Collection.SchemaUri)
	stripPrefix(&spec.RegisterSchemaUri)
	for i := range spec.Transforms {
		stripPrefix(&spec.Transforms[i].Shuffle.SourceSchemaUri)
	}
	cupaloy.SnapshotT(t, spec)
}

func TestLoadJournalRules(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	rules, err := catalog.LoadJournalRules()
	require.NoError(t, err)

	cupaloy.SnapshotT(t, rules)
}

func TestLoadTests(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	tests, err := catalog.LoadTests()
	require.NoError(t, err)

	cupaloy.SnapshotT(t, tests[0])
}

func TestLoadSchemaBundle(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	bundle, err := catalog.LoadSchemaBundle()
	require.NoError(t, err)
	require.NotEmpty(t, bundle.Bundle)
}

func TestLoadNPMPackage(t *testing.T) {
	var catalog, err = NewCatalog("../../catalog.db", "")
	require.NoError(t, err)
	data, err := catalog.LoadNPMPackage()
	require.NoError(t, err)

	require.True(t, len(data) > 0)
}

func stripPrefix(s *string) {
	(*s) = (*s)[strings.Index(*s, "/examples/"):]
}
