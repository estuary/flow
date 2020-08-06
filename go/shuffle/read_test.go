package shuffle

import (
	"container/heap"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

func TestReadBuilding(t *testing.T) {
	var (
		allJournals, allTransforms = buildReadTestJournalsAndTransforms()
		curJournals                = []pb.ListResponse_Journal{}
		curTransforms              = allTransforms
		ring                       = pf.Ring{
			Name:    "a-ring",
			Members: []pf.Ring_Member{{}, {}, {}},
		}
		rb = &ReadBuilder{
			service:    nil, // Not used in this test.
			transforms: func() []pf.TransformSpec { return curTransforms },
			ring:       func() pf.Ring { return ring },
			ringIndex:  2,
			listJournals: func(req pb.ListRequest) *pb.ListResponse {
				require.Equal(t, pb.LabelSelector{
					Include: pb.MustLabelSet(labels.Collection, "foo"),
				}, req.Selector)

				return &pb.ListResponse{Journals: curJournals}
			},
			listFragments:    func(pb.FragmentsRequest) (*pb.FragmentsResponse, error) { return &pb.FragmentsResponse{}, nil },
			journalsUpdateCh: nil, // Not used in this test.
		}
		existing = map[pb.Journal]*read{}
	)

	var toKeys = func(m map[pb.Journal]*read) (out []string) {
		for j, r := range m {
			require.Equal(t, j, r.spec.Name)
			require.Equal(t, j, r.req.Config.Journal)
			out = append(out, j.String())
		}
		sort.Strings(out)
		return
	}

	// Case: empty journals results in no built reads.
	var added, drain = rb.buildReads(existing, nil)
	require.Empty(t, drain)
	require.Empty(t, added)

	// Case: one journal & one transform => one read.
	curJournals, curTransforms = allJournals[:1], allTransforms[:1]
	const aJournal = "foo/bar=1/baz=abc/part=00?derivation=der&transform=bar-one"

	added, drain = rb.buildReads(existing, pb.Offsets{aJournal: 1122})
	require.Empty(t, drain)
	require.Equal(t, map[pb.Journal]*read{
		aJournal: {
			spec: pb.JournalSpec{
				Name:     aJournal,
				LabelSet: allJournals[0].Spec.LabelSet,
			},
			req: pf.ShuffleRequest{
				Config: pf.ShuffleConfig{
					Journal:     aJournal,
					Ring:        ring,
					Coordinator: 0,
					Shuffle:     allTransforms[0].Shuffle,
				},
				RingIndex: 2,
				Offset:    1122,
			},
			pollAdjust: 60e7 << 4, // 60 seconds as a message.Clock.
		},
	}, added)

	// Case: once the read exists, repeat invocations are no-ops.
	existing = added
	added, drain = rb.buildReads(existing, nil)
	require.Empty(t, drain)
	require.Empty(t, added)

	// Case: we can build a replay-read of a specific journal.
	var r, err = rb.buildReplayRead(aJournal, 1000, 2000)
	require.NoError(t, err)
	require.Equal(t, &read{
		spec: pb.JournalSpec{
			Name:     aJournal,
			LabelSet: allJournals[0].Spec.LabelSet,
		},
		req: pf.ShuffleRequest{
			Config: pf.ShuffleConfig{
				Journal:     aJournal,
				Ring:        ring,
				Coordinator: 0,
				Shuffle:     allTransforms[0].Shuffle,
			},
			RingIndex: 2,
			Offset:    1000,
			EndOffset: 2000,
		},
		pollAdjust: 0,
	}, r)

	// Case: if the configuration changes, the existing *read
	// is drained so that it may be restarted.
	allTransforms[0].Shuffle.ReadDelaySeconds++
	added, drain = rb.buildReads(existing, nil)
	require.Equal(t, []string{aJournal}, toKeys(drain))
	require.Empty(t, added)

	allTransforms[0].Shuffle.ReadDelaySeconds-- // Reset.

	// Case: if membership changes, we'll add and drain *reads as needed.
	curJournals, curTransforms = allJournals[1:], allTransforms
	added, drain = rb.buildReads(existing, nil)
	require.Equal(t, []string{aJournal}, toKeys(drain))
	require.Equal(t, []string{
		"foo/bar=1/baz=abc/part=01?derivation=der&transform=bar-one",
		"foo/bar=1/baz=def/part=00?derivation=der&transform=bar-one",
		"foo/bar=1/baz=def/part=00?derivation=der&transform=baz-def",
		"foo/bar=2/baz=def/part=00?derivation=der&transform=baz-def",
		"foo/bar=2/baz=def/part=01?derivation=der&transform=baz-def",
	}, toKeys(added))
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

	var env, err = r.Next()
	require.NoError(t, err)
	require.Equal(t, env.Journal.Name, pb.Journal("a/journal"))
	require.Equal(t, env.Begin, int64(0))
	require.Equal(t, env.End, int64(100))
	require.Equal(t, env.Message.(pf.IndexedShuffleResponse).Index, 0)

	env, err = r.Next()
	require.NoError(t, err)
	require.Equal(t, env.Begin, int64(200))
	require.Equal(t, env.End, int64(300))
	require.Equal(t, env.Message.(pf.IndexedShuffleResponse).Index, 1)

	env, err = r.Next()
	require.NoError(t, err)
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

func TestBuildAndApplyFragmentBounds(t *testing.T) {
	var t1 message.Clock
	t1.Update(time.Unix(11111111, 0))

	var cfg = pf.ShuffleConfig{
		Journal: "a/journal?query",
		Ring: pf.Ring{
			Members: []pf.Ring_Member{
				{MinMsgClock: t1},
				{},
			},
		},
	}

	// Index 0 has a MinMsgClock, which is projected into a BeginModTime.
	require.Equal(t,
		pb.FragmentsRequest{
			Journal:      "a/journal?query",
			BeginModTime: 11111051,
			PageLimit:    1,
		},
		buildFragmentBoundRequest(&pf.ShuffleRequest{
			Config:    cfg,
			RingIndex: 0,
		}))
	// Index 1 does not.
	require.Equal(t,
		pb.FragmentsRequest{
			Journal:   "a/journal?query",
			PageLimit: 1,
		},
		buildFragmentBoundRequest(&pf.ShuffleRequest{
			Config:    cfg,
			RingIndex: 1,
		}))

	// Empty response: no-op.
	var req = pf.ShuffleRequest{Offset: 1234}
	applyFragmentBoundResponse(&req, &pb.FragmentsResponse{})
	require.Equal(t, req.Offset, int64(1234))

	// Lower offset bound: no-op.
	applyFragmentBoundResponse(&req, &pb.FragmentsResponse{
		Fragments: []pb.FragmentsResponse__Fragment{{Spec: pb.Fragment{Begin: 1000}}},
	})
	require.Equal(t, req.Offset, int64(1234))

	// Higher offset bound: steps forward request Offset.
	applyFragmentBoundResponse(&req, &pb.FragmentsResponse{
		Fragments: []pb.FragmentsResponse__Fragment{{Spec: pb.Fragment{Begin: 2345}}},
	})
	require.Equal(t, req.Offset, int64(2345))
}

func TestLogicalPartitionGroupingAndReadWalking(t *testing.T) {
	var journals, transforms = buildReadTestJournalsAndTransforms()

	require.Equal(t, []struct{ begin, end int }{
		{begin: 0, end: 2},
		{begin: 2, end: 3},
		{begin: 3, end: 5},
	}, groupLogicalPartitions(journals))

	// Expect coordinators align with physical partitions of logical groups.
	var expect = []struct {
		journal     string
		transform   string
		coordinator int
	}{
		{"foo/bar=1/baz=abc/part=00?derivation=der&transform=bar-one", "bar-one", 0},
		{"foo/bar=1/baz=abc/part=01?derivation=der&transform=bar-one", "bar-one", 1},
		{"foo/bar=1/baz=def/part=00?derivation=der&transform=bar-one", "bar-one", 0},
		{"foo/bar=1/baz=def/part=00?derivation=der&transform=baz-def", "baz-def", 0},
		{"foo/bar=2/baz=def/part=00?derivation=der&transform=baz-def", "baz-def", 0},
		{"foo/bar=2/baz=def/part=01?derivation=der&transform=baz-def", "baz-def", 1},
	}
	walkReads(3, journals, transforms,
		func(spec pb.JournalSpec, transform pf.TransformSpec, coordinator int) {
			require.Equal(t, spec.Name.String(), expect[0].journal)
			require.Equal(t, transform.Shuffle.Transform.String(), expect[0].transform)
			require.Equal(t, coordinator, coordinator)
			expect = expect[1:]
		})
	require.Empty(t, expect)
}

func buildReadTestJournalsAndTransforms() ([]pb.ListResponse_Journal, []pf.TransformSpec) {
	var journals []pb.ListResponse_Journal

	for _, j := range []struct {
		bar  string
		baz  string
		part int
	}{
		{"1", "abc", 0}, // foo/bar=1/baz=abc/part=00
		{"1", "abc", 1}, // foo/bar=1/baz=abc/part=01
		{"1", "def", 0}, // foo/bar=1/baz=def/part=00
		{"2", "def", 0}, // foo/bar=2/baz=def/part=00
		{"2", "def", 1}, // foo/bar=2/baz=def/part=01
	} {
		var name = fmt.Sprintf("foo/bar=%s/baz=%s/part=%02d", j.bar, j.baz, j.part)

		journals = append(journals, pb.ListResponse_Journal{
			Spec: pb.JournalSpec{
				Name: pb.Journal(name),
				LabelSet: pb.MustLabelSet(
					labels.Collection, "foo",
					labels.FieldPrefix+"bar", j.bar,
					labels.FieldPrefix+"baz", j.baz,
				),
			},
		})
	}

	// Transforms reading partitions of "foo" into derivation "der".
	var transforms = []pf.TransformSpec{
		{
			Shuffle: pf.Shuffle{
				Transform:        "bar-one",
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
			Shuffle: pf.Shuffle{Transform: "baz-def"},
			Source: pf.TransformSpec_Source{
				Name: "foo",
				Partitions: pb.LabelSelector{
					Include: pb.MustLabelSet(labels.FieldPrefix+"baz", "def"),
				},
			},
			Derivation: pf.TransformSpec_Derivation{Name: "der"},
		},
		{
			Shuffle: pf.Shuffle{Transform: "unmatched"},
			Source: pf.TransformSpec_Source{
				Name: "foo",
				Partitions: pb.LabelSelector{
					Include: pb.MustLabelSet(labels.FieldPrefix+"baz", "other-value"),
				},
			},
			Derivation: pf.TransformSpec_Derivation{Name: "der"},
		},
	}
	return journals, transforms
}
