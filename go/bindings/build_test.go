package bindings

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/capture"
	"github.com/estuary/flow/go/materialize"
	pf "github.com/estuary/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuildCatalog(t *testing.T) {
	var tempdir = t.TempDir()
	built, err := BuildCatalog(BuildArgs{
		Context:             context.Background(),
		FileRoot:            "./testdata",
		CaptureDriverFn:     capture.NewDriver,
		MaterializeDriverFn: materialize.NewDriver,
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///build.flow.yaml",
			SourceType:  pf.ContentType_CATALOG_SPEC,
			CatalogPath: filepath.Join(tempdir, "catalog.db"),
		}})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	built.Config.CatalogPath = "/stable/path" // Blank |tempdir|.
	built.UUID = uuid.NameSpaceURL            // Stable, arbitrary fixture.
	cupaloy.SnapshotT(t, built)
}

func TestBuildSchema(t *testing.T) {
	var tempdir = t.TempDir()
	built, err := BuildCatalog(BuildArgs{
		Context:             context.Background(),
		FileRoot:            "./testdata",
		CaptureDriverFn:     nil, // Not needed.
		MaterializeDriverFn: nil, // Not needed.
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///b.schema.yaml",
			SourceType:  pf.ContentType_JSON_SCHEMA,
			CatalogPath: filepath.Join(tempdir, "catalog.db"),
		}})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	built.Config.CatalogPath = "/stable/path" // Blank |tempdir|.
	built.UUID = uuid.NameSpaceURL            // Stable, arbitrary fixture.
	cupaloy.SnapshotT(t, built)
}

func TestCatalogSchema(t *testing.T) {
	var schema = CatalogJSONSchema()
	require.True(t, len(schema) > 100)
}
