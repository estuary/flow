package shuffle

import (
	"container/heap"
	"fmt"
	"sort"
	"testing"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/mvcc/mvccpb"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
)

func TestReadBuilding(t *testing.T) {
	var (
		allJournals, allShards, allTransforms = buildReadTestJournalsAndTransforms()
		ranges                                = labels.MustParseRangeSpec(allShards[0].LabelSet)
		readerSpecs                           = ReadSpecsFromTransforms(allTransforms)
		rb                                    = &ReadBuilder{
			service:    nil, // Not used in this test.
			ranges:     ranges,
			transforms: readerSpecs,
			members:    func() []*pc.ShardSpec { return allShards },
			journals:   &keyspace.KeySpace{Root: allJournals.Root},
		}
		existing = map[pb.Journal]*read{}
	)

	var toKeys = func(m map[pb.Journal]*read) (out []string) {
		for j, r := range m {
			require.Equal(t, j, r.spec.Name, "incorrect journalSpec name")
			require.Equal(t, j, r.req.Shuffle.Journal, "incorrect shuffle journal name")
			out = append(out, j.String())
		}
		sort.Strings(out)
		return
	}

	// Case: empty journals results in no built reads.
	added, drain, err := rb.buildReads(existing, nil)
	require.NoError(t, err)
	require.Empty(t, drain)
	require.Empty(t, added)

	// Case: one journal & one transform => one read.
	rb.journals.KeyValues, rb.transforms = allJournals.KeyValues[:1], readerSpecs[:1]
	const aJournal = "foo/bar=1/baz=abc/part=00;transform/der/bar-one"

	added, drain, err = rb.buildReads(existing, pb.Offsets{aJournal: 1122})
	require.NoError(t, err)
	require.Empty(t, drain)
	require.Equal(t, map[pb.Journal]*read{
		aJournal: {
			spec: pb.JournalSpec{
				Name:     aJournal,
				LabelSet: allJournals.KeyValues[0].Decoded.(*pb.JournalSpec).LabelSet,
			},
			req: pf.ShuffleRequest{
				Shuffle: pf.JournalShuffle{
					Journal:     aJournal,
					Coordinator: "shard/0",
					Shuffle:     allTransforms[0].Shuffle,
					Replay:      false,
				},
				Range:  ranges,
				Offset: 1122,
			},
			resp:       pf.IndexedShuffleResponse{Transform: &readerSpecs[0]},
			pollAdjust: 60e7 << 4, // 60 seconds as a message.Clock.
		},
	}, added)

	// Case: once the read exists, repeat invocations are no-ops.
	existing = added
	added, drain, err = rb.buildReads(existing, nil)
	require.NoError(t, err)
	require.Empty(t, drain)
	require.Empty(t, added)

	// Case: we can build a replay-read of a specific journal.
	r, err := rb.buildReplayRead(aJournal, 1000, 2000)
	require.NoError(t, err)
	require.Equal(t, &read{
		spec: pb.JournalSpec{
			Name:     aJournal,
			LabelSet: allJournals.KeyValues[0].Decoded.(*pb.JournalSpec).LabelSet,
		},
		req: pf.ShuffleRequest{
			Shuffle: pf.JournalShuffle{
				Journal:     aJournal,
				Coordinator: "shard/0",
				Shuffle:     allTransforms[0].Shuffle,
				Replay:      true,
			},
			Range:     ranges,
			Offset:    1000,
			EndOffset: 2000,
		},
		resp:       pf.IndexedShuffleResponse{Transform: &readerSpecs[0]},
		pollAdjust: 0,
	}, r)

	// Case: if the configuration changes, the existing *read
	// is drained so that it may be restarted.
	readerSpecs[0].Shuffle.ReadDelaySeconds++
	added, drain, err = rb.buildReads(existing, nil)
	require.NoError(t, err)
	require.Equal(t, []string{aJournal}, toKeys(drain))
	require.Empty(t, added)

	readerSpecs[0].Shuffle.ReadDelaySeconds-- // Reset.

	// Case: if membership changes, we'll add and drain *reads as needed.
	rb.journals.KeyValues, rb.transforms = allJournals.KeyValues[1:], readerSpecs
	added, drain, err = rb.buildReads(existing, nil)
	require.NoError(t, err)
	require.Equal(t, []string{aJournal}, toKeys(drain))
	require.Equal(t, []string{
		"foo/bar=1/baz=abc/part=01;transform/der/bar-one",
		"foo/bar=1/baz=def/part=00;transform/der/bar-one",
		"foo/bar=1/baz=def/part=00;transform/der/baz-def",
		"foo/bar=2/baz=def/part=00;transform/der/baz-def",
		"foo/bar=2/baz=def/part=01;transform/der/baz-def",
	}, toKeys(added))

	// ReadThrough filters Offsets to journals of this readBuilder.
	offsets, err := rb.ReadThrough(pb.Offsets{
		// Matches on journal name & metadata.
		"foo/bar=1/baz=def/part=00;transform/der/baz-def": 12,
		"foo/bar=2/baz=def/part=01;transform/der/baz-def": 34,
		// Matches on journal name (only).
		"foo/bar=1/baz=abc/part=01": 56,
		"foo/bar=1/baz=def/part=00": 78,
		"missing":                   9999,
	})
	require.NoError(t, err)
	require.Equal(t, offsets, pb.Offsets{
		"foo/bar=1/baz=def/part=00;transform/der/baz-def": 12,
		"foo/bar=2/baz=def/part=01;transform/der/baz-def": 34,
		"foo/bar=1/baz=abc/part=01;transform/der/bar-one": 56,
		"foo/bar=1/baz=def/part=00;transform/der/bar-one": 78,
	})
}

func TestReadIteration(t *testing.T) {
	var r = &read{
		spec: pb.JournalSpec{
			Name: "a/journal",
		},
		resp: pf.IndexedShuffleResponse{
			Index: 0,
			ShuffleResponse: &pf.ShuffleResponse{
				Begin: []int64{0, 200, 400},
				End:   []int64{100, 300, 500},
			},
		},
	}

	var env = r.dequeue()
	require.Equal(t, env.Journal.Name, pb.Journal("a/journal"))
	require.Equal(t, env.Begin, int64(0))
	require.Equal(t, env.End, int64(100))
	require.Equal(t, env.Message.(pf.IndexedShuffleResponse).Index, 0)

	env = r.dequeue()
	require.Equal(t, env.Begin, int64(200))
	require.Equal(t, env.End, int64(300))
	require.Equal(t, env.Message.(pf.IndexedShuffleResponse).Index, 1)

	env = r.dequeue()
	require.Equal(t, env.Begin, int64(400))
	require.Equal(t, env.End, int64(500))
	require.Equal(t, env.Message.(pf.IndexedShuffleResponse).Index, 2)

	require.Equal(t, r.resp.Index, len(r.resp.Begin))
}

func TestReadHeaping(t *testing.T) {
	var resp = &pf.ShuffleResponse{
		UuidParts: []pf.UUIDParts{
			{Clock: 2000},
			{Clock: 1001},
			{Clock: 1002},
			{Clock: 2003},
			{Clock: 1004},
			{Clock: 1005},
		},
	}
	var h readHeap

	// Push reads in a mixed order.
	for _, r := range []*read{
		{resp: pf.IndexedShuffleResponse{Index: 3, ShuffleResponse: resp}, pollAdjust: 1000},
		{resp: pf.IndexedShuffleResponse{Index: 1, ShuffleResponse: resp}, pollAdjust: 2000},
		{resp: pf.IndexedShuffleResponse{Index: 0, ShuffleResponse: resp}, pollAdjust: 1000},
		{resp: pf.IndexedShuffleResponse{Index: 5, ShuffleResponse: resp}, pollAdjust: 2000},
		{resp: pf.IndexedShuffleResponse{Index: 2, ShuffleResponse: resp}, pollAdjust: 2000},
		{resp: pf.IndexedShuffleResponse{Index: 4, ShuffleResponse: resp}, pollAdjust: 2000},
	} {
		heap.Push(&h, r)
	}

	// Expect to pop reads in Index order, after adjusting for |pollAdjust|.
	for ind := 0; ind != 6; ind++ {
		require.Equal(t, ind, heap.Pop(&h).(*read).resp.Index)
	}
	require.Empty(t, h)
}

func TestCoordinatorAssignment(t *testing.T) {
	var journals, shards, transforms = buildReadTestJournalsAndTransforms()
	readerSpecs := ReadSpecsFromTransforms(transforms)

	// Expect coordinators align with physical partitions of logical groups.
	var expect = []struct {
		journal     string
		transform   string
		coordinator pc.ShardID
	}{
		{"foo/bar=1/baz=abc/part=00;transform/der/bar-one", "bar-one", "shard/0"},
		{"foo/bar=1/baz=abc/part=01;transform/der/bar-one", "bar-one", "shard/2"},
		{"foo/bar=1/baz=def/part=00;transform/der/bar-one", "bar-one", "shard/0"},
		{"foo/bar=1/baz=def/part=00;transform/der/baz-def", "baz-def", "shard/2"},
		{"foo/bar=2/baz=def/part=00;transform/der/baz-def", "baz-def", "shard/0"},
		{"foo/bar=2/baz=def/part=01;transform/der/baz-def", "baz-def", "shard/1"},
	}
	var err = walkReads(shards, journals, readerSpecs,
		func(spec pb.JournalSpec, transform pf.ReadSpec, coordinator pc.ShardID) {
			require.Equal(t, expect[0].journal, spec.Name.String())
			// TODO: Do something cleaner than just indexing into the ReaderLabels
			require.Equal(t, expect[0].transform, transform.ReaderNames[1])
			require.Equal(t, expect[0].coordinator, coordinator)
			expect = expect[1:]
		})
	require.NoError(t, err)
	require.Empty(t, expect)
}

func TestHRWRegression(t *testing.T) {
	var ring = []uint32{
		hashString("Foo"),
		hashString("Bar"),
		hashString("Bez"),
		hashString("Qib"),
	}
	var h = hashString("Test")

	require.Equal(t, []uint32{0xc7e1677, 0xdbbc7dba, 0xcbb36a2e, 0x5389fa17}, ring)
	require.Equal(t, uint32(0x2ffcbe05), h)

	require.Equal(t, 1, pickHRW(h, ring, 0, 4))
	require.Equal(t, 0, pickHRW(h, ring, 0, 1))
	require.Equal(t, 1, pickHRW(h, ring, 1, 4))
	require.Equal(t, 2, pickHRW(h, ring, 2, 4))
	require.Equal(t, 2, pickHRW(h, ring, 2, 3))
}

func buildReadTestJournalsAndTransforms() (*keyspace.KeySpace, []*pc.ShardSpec, []pf.TransformSpec) {
	var journals = &keyspace.KeySpace{
		Root: "/the/journals",
	}

	for _, j := range []struct {
		bar   string
		baz   string
		begin string
		end   string
		part  int
	}{
		{"1", "abc", "aa", "cc", 0}, // foo/bar=1/baz=abc/part=00
		{"1", "abc", "cc", "ff", 1}, // foo/bar=1/baz=abc/part=01
		{"1", "def", "aa", "ff", 0}, // foo/bar=1/baz=def/part=00
		{"2", "def", "aa", "bb", 0}, // foo/bar=2/baz=def/part=00
		{"2", "def", "bb", "ff", 1}, // foo/bar=2/baz=def/part=01
	} {
		var name = fmt.Sprintf("foo/bar=%s/baz=%s/part=%02d", j.bar, j.baz, j.part)

		journals.KeyValues = append(journals.KeyValues, keyspace.KeyValue{
			Raw: mvccpb.KeyValue{
				Key: append(append([]byte(journals.Root), '/'), name...),
			},
			Decoded: &pb.JournalSpec{
				Name: pb.Journal(name),
				LabelSet: pb.MustLabelSet(
					labels.Collection, "foo",
					labels.FieldPrefix+"bar", j.bar,
					labels.FieldPrefix+"baz", j.baz,
					labels.KeyBegin, j.begin,
					labels.KeyEnd, j.end,
				),
			},
		})
	}
	var shards = []*pc.ShardSpec{
		{Id: "shard/0", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "aa",
			labels.KeyEnd, "bb",
			labels.RClockBegin, "0000000000000000",
			labels.RClockEnd, "ffffffffffffffff")},
		{Id: "shard/1", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "bb",
			labels.KeyEnd, "cc",
			labels.RClockBegin, "0000000000000000",
			labels.RClockEnd, "ffffffffffffffff")},
		{Id: "shard/2", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "cc",
			labels.KeyEnd, "ff",
			labels.RClockBegin, "0000000000000000",
			labels.RClockEnd, "8000000000000000")},
	}

	// Transforms reading partitions of "foo" into derivation "der".
	var transforms = []pf.TransformSpec{
		{
			Name: "bar-one",
			Shuffle: pf.Shuffle{
				UsesSourceKey:    true,
				ReadDelaySeconds: 60,
			},
			Source: pf.TransformSpec_Source{
				Name: "foo",
				Partitions: pb.LabelSelector{
					Include: pb.MustLabelSet(labels.FieldPrefix+"bar", "1"),
				},
			},
			Derivation: pf.TransformSpec_Derivation{Name: "der"},
		},
		{
			Name: "baz-def",
			Shuffle: pf.Shuffle{
				UsesSourceKey: false,
			},
			Source: pf.TransformSpec_Source{
				Name: "foo",
				Partitions: pb.LabelSelector{
					Include: pb.MustLabelSet(labels.FieldPrefix+"baz", "def"),
				},
			},
			Derivation: pf.TransformSpec_Derivation{Name: "der"},
		},
		{
			Name:    "unmatched",
			Shuffle: pf.Shuffle{},
			Source: pf.TransformSpec_Source{
				Name: "foo",
				Partitions: pb.LabelSelector{
					Include: pb.MustLabelSet(labels.FieldPrefix+"baz", "other-value"),
				},
			},
			Derivation: pf.TransformSpec_Derivation{Name: "der"},
		},
	}
	return journals, shards, transforms
}
