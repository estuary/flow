package bindings

import (
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/materialize/driver"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestBuildCatalogNew(t *testing.T) {
	var tempdir = t.TempDir()
	built, err := BuildCatalog(BuildArgs{
		FileRoot:            "./testdata",
		MaterializeDriverFn: driver.NewDriver,
		BuildAPI_Config: pf.BuildAPI_Config{
			Directory:   "testdata",
			Source:      "file:///build.flow.yaml",
			CatalogPath: filepath.Join(tempdir, "catalog.db"),
			ExtraJournalRules: &pf.JournalRules{
				Rules: []pf.JournalRules_Rule{
					{
						Rule:     "demonstrate pass through",
						Template: pb.JournalSpec{Replication: 867},
					},
				},
			},
			ExtraShardRules: &pf.ShardRules{
				Rules: []pf.ShardRules_Rule{
					{
						Rule:     "here too",
						Template: pc.ShardSpec{HotStandbys: 5309},
					},
				},
			},
		}})
	require.NoError(t, err)
	require.Empty(t, built.Errors)

	built.Config.CatalogPath = "/stable/path" // Blank |tempdir|.
	cupaloy.SnapshotT(t, built)
}

func TestCatalogSchema(t *testing.T) {
	var schema = CatalogJSONSchema()
	require.True(t, len(schema) > 100)
}
