package flow

import (
	"context"
	"database/sql"
	"path"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/bindings"
	flowLabels "github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestBuildingSpecs(t *testing.T) {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "fixture",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///specs_test.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var collection *pf.CollectionSpec
	var derivation *pf.CollectionSpec

	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		if collection, err = catalog.LoadCollection(db, "example/collection"); err != nil {
			return err
		}
		derivation, err = catalog.LoadCollection(db, "example/derivation")
		return err
	}))

	// Build a data partition.
	var set pf.LabelSet
	set = flowLabels.EncodePartitionLabels(collection.PartitionFields, tuple.Tuple{true, "the str"}, set)
	set = flowLabels.EncodeHexU32Label(flowLabels.KeyBegin, 11223344, set)
	set = flowLabels.EncodeHexU32Label(flowLabels.KeyEnd, 66778899, set)

	partition, err := BuildPartitionSpec(collection.PartitionTemplate, set)
	require.NoError(t, err)

	// Build a derivation shard.
	set = pf.LabelSet{} // Clear.
	set = flowLabels.EncodeRange(pf.RangeSpec{
		KeyBegin:    11223344,
		KeyEnd:      55667788,
		RClockBegin: 31214151,
		RClockEnd:   61514131,
	}, set)

	shard, err := BuildShardSpec(derivation.Derivation.ShardTemplate, set)
	require.NoError(t, err)

	// Build a derivation shard that's currently splitting from its source.
	set.AddValue(flowLabels.SplitSource, "something/something")

	shardSplitSource, err := BuildShardSpec(derivation.Derivation.ShardTemplate, set)
	require.NoError(t, err)

	// Build a recovery log.
	var recovery = BuildRecoverySpec(derivation.Derivation.RecoveryLogTemplate, shard)

	// Snapshot all specs.
	cupaloy.SnapshotT(t,
		"PARTITION:\n\n"+
			proto.MarshalTextString(partition)+
			"\n\nSHARD:\n\n"+
			proto.MarshalTextString(shard)+
			"\n\nSHARD (split-source):\n\n"+
			proto.MarshalTextString(shardSplitSource)+
			"\n\nRECOVERY:\n\n"+
			proto.MarshalTextString(recovery),
	)

	// Expect we can re-build specifications from their existing states,
	// and that we recover identical specs when we do so.

	partition2, err := BuildPartitionSpec(collection.PartitionTemplate, partition.LabelSet)
	require.NoError(t, err)
	require.Equal(t, partition, partition2)

	shard2, err := BuildShardSpec(derivation.Derivation.ShardTemplate, shard.LabelSet)
	require.NoError(t, err)
	require.Equal(t, shard, shard2)

	shard2, err = BuildShardSpec(derivation.Derivation.ShardTemplate, shardSplitSource.LabelSet)
	require.NoError(t, err)
	require.Equal(t, shardSplitSource, shard2)

	var recovery2 = BuildRecoverySpec(derivation.Derivation.RecoveryLogTemplate, shard2)
	require.Equal(t, recovery, recovery2)
}
