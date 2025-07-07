package flow

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc"
)

func TestConvergence(t *testing.T) {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "5555555555555555",
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

	// Create many partition, shard, and recovery log fixtures.

	partitionSpec1, err := BuildPartitionSpec(collection.PartitionTemplate,
		labels.EncodePartitionLabels(
			collection.PartitionFields, tuple.Tuple{true, "a-val"},
			pb.MustLabelSet(
				labels.KeyBegin, fmt.Sprintf("%08x", 0x10000000),
				labels.KeyEnd, fmt.Sprintf("%08x", 0x3fffffff),
			)))
	require.NoError(t, err)

	partitionSpec2, err := BuildPartitionSpec(collection.PartitionTemplate,
		labels.EncodePartitionLabels(
			collection.PartitionFields, tuple.Tuple{true, "a-val"},
			pb.MustLabelSet(
				labels.KeyBegin, fmt.Sprintf("%08x", 0x40000000),
				labels.KeyEnd, fmt.Sprintf("%08x", 0x5fffffff),
			)))
	require.NoError(t, err)

	partitionSpec3, err := BuildPartitionSpec(collection.PartitionTemplate,
		labels.EncodePartitionLabels(
			collection.PartitionFields, tuple.Tuple{false, "other-val"},
			pb.MustLabelSet(
				labels.KeyBegin, labels.KeyBeginMin,
				labels.KeyEnd, labels.KeyEndMax,
			)))
	require.NoError(t, err)

	shardSpec1, err := BuildShardSpec(derivation.Derivation.ShardTemplate,
		labels.EncodeRange(pf.RangeSpec{
			KeyBegin:    0x10000000,
			KeyEnd:      0x2fffffff,
			RClockBegin: 0x60000000,
			RClockEnd:   0x9fffffff,
		}, pf.LabelSet{}),
	)
	require.NoError(t, err)
	logSpec1 := BuildRecoverySpec(derivation.Derivation.RecoveryLogTemplate, shardSpec1)

	shardSpec2, err := BuildShardSpec(derivation.Derivation.ShardTemplate,
		labels.EncodeRange(pf.RangeSpec{
			KeyBegin:    0x30000000,
			KeyEnd:      0x3fffffff,
			RClockBegin: 0x60000000,
			RClockEnd:   0x7fffffff,
		}, pf.LabelSet{}),
	)
	require.NoError(t, err)
	logSpec2 := BuildRecoverySpec(derivation.Derivation.RecoveryLogTemplate, shardSpec2)

	shardSpec3, err := BuildShardSpec(derivation.Derivation.ShardTemplate,
		labels.EncodeRange(pf.RangeSpec{
			KeyBegin:    0x30000000,
			KeyEnd:      0x3fffffff,
			RClockBegin: 0x80000000,
			RClockEnd:   0x9fffffff,
		}, pf.LabelSet{}),
	)
	require.NoError(t, err)
	logSpec3 := BuildRecoverySpec(derivation.Derivation.RecoveryLogTemplate, shardSpec3)

	var allPartitions = []pb.ListResponse_Journal{
		{Spec: *partitionSpec1, ModRevision: 11},
		{Spec: *partitionSpec2, ModRevision: 22},
		{Spec: *partitionSpec3, ModRevision: 33},
	}
	var allShards = []pc.ListResponse_Shard{
		{Spec: *shardSpec1, ModRevision: 44},
		{Spec: *shardSpec2, ModRevision: 55},
		{Spec: *shardSpec3, ModRevision: 66},
	}
	var allLogs = []pb.ListResponse_Journal{
		{Spec: *logSpec1, ModRevision: 77},
		{Spec: *logSpec2, ModRevision: 88},
		{Spec: *logSpec3, ModRevision: 99},
	}

	t.Run("list-shards-request", func(t *testing.T) {
		var out = ListShardsRequest(derivation)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("list-recovery-logs-request", func(t *testing.T) {
		var out = ListRecoveryLogsRequest(derivation)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("list-partitions-request", func(t *testing.T) {
		var out = ListPartitionsRequest(collection)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("partitions-to-current-splits", func(t *testing.T) {
		var out = MapPartitionsToCurrentSplits(allPartitions)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("shards-to-current-splits", func(t *testing.T) {
		var out = MapShardsToCurrentOrInitialSplits(allShards, 100)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("shards-to-initial-splits", func(t *testing.T) {
		var out = MapShardsToCurrentOrInitialSplits(nil, 4)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("shard-split-on-key", func(t *testing.T) {
		var out, err = MapShardToSplit(derivation, allShards[0:1], true)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("shard-split-on-r-clock", func(t *testing.T) {
		var out, err = MapShardToSplit(derivation, allShards[1:2], false)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("shard-split-errors", func(t *testing.T) {
		var shard, err = BuildShardSpec(derivation.Derivation.ShardTemplate,
			labels.EncodeRange(pf.RangeSpec{
				KeyEnd:    0x10000000,
				RClockEnd: 0x10000000,
			}, pf.LabelSet{}))
		require.NoError(t, err)

		// Case: already has a split-source.
		shard.LabelSet.SetValue(labels.SplitSource, "whoops")
		_, err = MapShardToSplit(derivation, []pc.ListResponse_Shard{{Spec: *shard}}, false)
		require.EqualError(t, err,
			"shard derivation/example/derivation/ffffffffffffffff/00000000-00000000 is already splitting from source whoops")

		// Case: already has a split-target.
		shard.LabelSet.Remove(labels.SplitSource)
		shard.LabelSet.SetValue(labels.SplitTarget, "whoops")

		_, err = MapShardToSplit(derivation, []pc.ListResponse_Shard{{Spec: *shard}}, false)
		require.EqualError(t, err,
			"shard derivation/example/derivation/ffffffffffffffff/00000000-00000000 is already splitting into target whoops")

		// Case: expects exactly one shard.
		_, err = MapShardToSplit(derivation, nil, false)
		require.EqualError(t, err, "expected exactly one shard in the response")
		_, err = MapShardToSplit(derivation, allShards, false)
		require.EqualError(t, err, "expected exactly one shard in the response")
	})

	t.Run("partitions-split-sub-range", func(t *testing.T) {
		var out, err = MapPartitionToSplit(collection, allPartitions[0:1], 2)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("partitions-split-full-range", func(t *testing.T) {
		var out, err = MapPartitionToSplit(collection, allPartitions[2:3], 8)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("partitions-split-errors", func(t *testing.T) {
		// Case: expects exactly one partition.
		_, err = MapPartitionToSplit(collection, nil, 2)
		require.EqualError(t, err, "expected exactly one journal in the response")
		_, err = MapPartitionToSplit(collection, allPartitions, 2)
		require.EqualError(t, err, "expected exactly one journal in the response")

		// Case: not a power-of-two in [2, 256].
		for _, splits := range []uint{0, 1, 3, 7, 127, 258, 512} {
			_, err = MapPartitionToSplit(collection, allPartitions[0:1], splits)
			require.EqualError(t, err, "splits must be a power of two and in range [2, 256]")
		}
	})

	var desiredPartitions = MapPartitionsToCurrentSplits(allPartitions)

	// aj & as are empty fixtures which verify that routines are correctly
	// appending into the passed-in values.
	var aj = []pb.ApplyRequest_Change{{Delete: "journal-prelude"}}
	var as = []pc.ApplyRequest_Change{{Delete: "shard-prelude"}}

	t.Run("collection-has-no-changes", func(t *testing.T) {
		var out, err = CollectionChanges(collection, allPartitions, desiredPartitions, aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("collection-insertion", func(t *testing.T) {
		var out, err = CollectionChanges(collection, allPartitions[1:], desiredPartitions, aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("collection-deletion", func(t *testing.T) {
		var out, err = CollectionChanges(collection, allPartitions, desiredPartitions[1:], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("collection-update", func(t *testing.T) {
		var modified = append([]pb.ListResponse_Journal(nil), allPartitions...)
		modified[0].Spec.Replication = 42

		var out, err = CollectionChanges(collection, modified, desiredPartitions, aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, out)
	})

	t.Run("collection-duplicate", func(t *testing.T) {
		var duplicated = append(desiredPartitions, desiredPartitions[0])

		var _, err = CollectionChanges(collection, allPartitions, duplicated, aj[:1])
		require.EqualError(t, err,
			"duplicate desired partition journal example/collection/ffffffffffffffff/a_bool=%_true/a_str=a-val/pivot=10000000")
	})

	var desiredTasks = MapShardsToCurrentOrInitialSplits(allShards, 1234)

	t.Run("task-has-no-changes", func(t *testing.T) {
		var shards, journals, err = TaskChanges(derivation, allShards, allLogs, desiredTasks, as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-shard-insertion", func(t *testing.T) {
		var shards, journals, err = TaskChanges(derivation, allShards[1:], allLogs, desiredTasks, as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-log-insertion", func(t *testing.T) {
		var shards, journals, err = TaskChanges(derivation, allShards, allLogs[1:], desiredTasks, as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-shard-deletion", func(t *testing.T) {
		var shards, journals, err = TaskChanges(derivation, allShards, allLogs[1:], desiredTasks[1:], as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-log-deletion", func(t *testing.T) {
		var shards, journals, err = TaskChanges(derivation, allShards[1:], allLogs, desiredTasks[1:], as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-shard-update", func(t *testing.T) {
		var modified = append([]pc.ListResponse_Shard(nil), allShards...)
		modified[0].Spec.HotStandbys = 42

		var shards, journals, err = TaskChanges(derivation, modified, allLogs, desiredTasks, as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-log-update", func(t *testing.T) {
		var modified = append([]pb.ListResponse_Journal(nil), allLogs...)
		modified[0].Spec.Replication = 42

		var shards, journals, err = TaskChanges(derivation, allShards, modified, desiredTasks, as[:1], aj[:1])
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("task-duplicate", func(t *testing.T) {
		var duplicated = append(desiredTasks, desiredTasks[0])

		var _, _, err = TaskChanges(derivation, allShards, allLogs, duplicated, as[:1], aj[:1])
		require.EqualError(t, err,
			"duplicate desired shard derivation/example/derivation/ffffffffffffffff/10000000-60000000")
	})

	t.Run("activate-empty-cluster", func(t *testing.T) {
		var ctx = context.Background()
		var jc = &mockJournals{}
		var sc = &mockShards{}

		var shards, journals, err = ActivationChanges(ctx, jc, sc, []*pf.CollectionSpec{collection}, []pf.Task{derivation}, 2)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("activate-has-no-changes", func(t *testing.T) {
		var ctx = context.Background()
		var jc = &mockJournals{
			collections: map[string]*pb.ListResponse{
				collection.Name.String(): {Journals: allPartitions},
			},
			logs: map[string]*pb.ListResponse{
				derivation.TaskName(): {Journals: allLogs},
			},
		}
		var sc = &mockShards{
			tasks: map[string]*pc.ListResponse{
				derivation.TaskName(): {Shards: allShards},
			},
		}

		var shards, journals, err = ActivationChanges(ctx, jc, sc, []*pf.CollectionSpec{collection}, []pf.Task{derivation}, 2)
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("deletion-empty-cluster", func(t *testing.T) {
		var ctx = context.Background()
		var jc = &mockJournals{}
		var sc = &mockShards{}

		var shards, journals, err = DeletionChanges(ctx, jc, sc, []*pf.CollectionSpec{collection}, []pf.Task{derivation})
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})

	t.Run("deletion-full-cluster", func(t *testing.T) {
		var ctx = context.Background()
		var jc = &mockJournals{
			collections: map[string]*pb.ListResponse{
				collection.Name.String(): {Journals: allPartitions},
			},
			logs: map[string]*pb.ListResponse{
				derivation.TaskName(): {Journals: allLogs},
			},
		}
		var sc = &mockShards{
			tasks: map[string]*pc.ListResponse{
				derivation.TaskName(): {Shards: allShards},
			},
		}

		var shards, journals, err = DeletionChanges(ctx, jc, sc, []*pf.CollectionSpec{collection}, []pf.Task{derivation})
		require.NoError(t, err)
		cupaloy.SnapshotT(t, shards, journals)
	})
}

type mockJournals struct {
	collections map[string]*pb.ListResponse
	logs        map[string]*pb.ListResponse
}

type mockJournalsListStream struct {
	out *pb.ListResponse
	pb.Journal_ListClient
}

type mockShards struct {
	tasks map[string]*pc.ListResponse
}

func (jc *mockJournals) List(ctx context.Context, in *pb.ListRequest, opts ...grpc.CallOption) (pb.Journal_ListClient, error) {
	var out *pb.ListResponse

	if name := in.Selector.Include.ValueOf(labels.Collection); name != "" {
		if r, ok := jc.collections[name]; ok {
			out = r
		} else {
			out = new(pb.ListResponse)
		}
	}

	if name := in.Selector.Include.ValueOf(labels.TaskName); name != "" {
		if r, ok := jc.logs[name]; ok {
			out = r
		} else {
			out = new(pb.ListResponse)
		}
	}

	if out == nil {
		return nil, fmt.Errorf("bad request")
	}

	out.Header = pb.Header{
		Etcd:  pb.Header_Etcd{ClusterId: 1, MemberId: 2, Revision: 3, RaftTerm: 4},
		Route: pb.Route{Primary: -1},
	}
	for i := range out.Journals {
		out.Journals[i].Route = pb.Route{Primary: -1}
	}
	return &mockJournalsListStream{out, nil}, nil
}

func (ls *mockJournalsListStream) Recv() (*pb.ListResponse, error) {
	var out = ls.out
	ls.out = nil

	if out != nil {
		return out, nil
	} else {
		return nil, io.EOF
	}
}
func (ls *mockJournalsListStream) Context() context.Context { return context.Background() }

func (sc *mockShards) List(ctx context.Context, in *pc.ListRequest, opts ...grpc.CallOption) (*pc.ListResponse, error) {
	var out *pc.ListResponse

	if name := in.Selector.Include.ValueOf(labels.TaskName); name != "" {
		if r, ok := sc.tasks[name]; ok {
			out = r
		} else {
			out = new(pc.ListResponse)
		}
	}

	if out == nil {
		return nil, fmt.Errorf("bad request")
	}

	out.Header = pb.Header{
		Etcd:  pb.Header_Etcd{ClusterId: 1, MemberId: 2, Revision: 3, RaftTerm: 4},
		Route: pb.Route{Primary: -1},
	}
	for i := range out.Shards {
		out.Shards[i].Route = pb.Route{Primary: -1}
	}
	return out, nil
}

var _ pb.JournalClient = &mockJournals{}

func (jc *mockJournals) Apply(ctx context.Context, in *pb.ApplyRequest, opts ...grpc.CallOption) (*pb.ApplyResponse, error) {
	panic("not implemented")
}
func (jc *mockJournals) Read(ctx context.Context, in *pb.ReadRequest, opts ...grpc.CallOption) (pb.Journal_ReadClient, error) {
	panic("not implemented")
}
func (jc *mockJournals) Append(ctx context.Context, opts ...grpc.CallOption) (pb.Journal_AppendClient, error) {
	panic("not implemented")
}
func (jc *mockJournals) Replicate(ctx context.Context, opts ...grpc.CallOption) (pb.Journal_ReplicateClient, error) {
	panic("not implemented")
}
func (jc *mockJournals) ListFragments(ctx context.Context, in *pb.FragmentsRequest, opts ...grpc.CallOption) (*pb.FragmentsResponse, error) {
	panic("not implemented")
}
func (jc *mockJournals) FragmentStoreHealth(ctx context.Context, in *pb.FragmentStoreHealthRequest, opts ...grpc.CallOption) (*pb.FragmentStoreHealthResponse, error) {
	panic("not implemented")
}

var _ pc.ShardClient = &mockShards{}

func (sc *mockShards) Stat(ctx context.Context, in *pc.StatRequest, opts ...grpc.CallOption) (*pc.StatResponse, error) {
	panic("not implemented")
}
func (sc *mockShards) Apply(ctx context.Context, in *pc.ApplyRequest, opts ...grpc.CallOption) (*pc.ApplyResponse, error) {
	panic("not implemented")
}
func (sc *mockShards) GetHints(ctx context.Context, in *pc.GetHintsRequest, opts ...grpc.CallOption) (*pc.GetHintsResponse, error) {
	panic("not implemented")
}
func (sc *mockShards) Unassign(ctx context.Context, in *pc.UnassignRequest, opts ...grpc.CallOption) (*pc.UnassignResponse, error) {
	panic("not implemented")
}
