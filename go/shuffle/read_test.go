package shuffle

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
)

func TestReadBuilding(t *testing.T) {
	var (
		allJournals, allShards, task = buildReadTestJournalsAndTransforms()
		ranges                       = labels.MustParseRangeSpec(allShards[0].LabelSet)
		shuffles                     = task.TaskShuffles()
		drainCh                      = make(chan struct{})
		rb, rbErr                    = NewReadBuilder(
			"build-id",
			drainCh,
			flow.Journals{KeySpace: &keyspace.KeySpace{Root: allJournals.Root}},
			localPublisher,
			nil, // Service is not used.
			allShards[0].Id,
			shuffles,
		)
		existing = map[pb.Journal]*read{}
	)
	require.NoError(t, rbErr)
	rb.members = func() []*pc.ShardSpec { return allShards }

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
	rb.journals.KeyValues, rb.shuffles = allJournals.KeyValues[:1], shuffles[:1]
	const aJournal = "foo/bar=1/baz=abc/part=00;transform/der/bar-one"

	added, drain, err = rb.buildReads(existing, pb.Offsets{aJournal: 1122})
	require.NoError(t, err)
	require.Empty(t, drain)
	require.Equal(t, map[pb.Journal]*read{
		aJournal: {
			publisher: localPublisher,
			spec: pb.JournalSpec{
				Name:     aJournal,
				LabelSet: allJournals.KeyValues[0].Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec).LabelSet,
			},
			req: pf.ShuffleRequest{
				Shuffle: pf.JournalShuffle{
					Journal:     aJournal,
					Coordinator: "shard/2",
					Shuffle:     shuffles[0],
					Replay:      false,
					BuildId:     "build-id",
				},
				Range:  ranges,
				Offset: 1122,
			},
			resp:      pf.IndexedShuffleResponse{Shuffle: shuffles[0]},
			readDelay: 60e7 << 4, // 60 seconds as a message.Clock.
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
		publisher: rb.publisher,
		spec: pb.JournalSpec{
			Name:     aJournal,
			LabelSet: allJournals.KeyValues[0].Decoded.(allocator.Item).ItemValue.(*pb.JournalSpec).LabelSet,
		},
		req: pf.ShuffleRequest{
			Shuffle: pf.JournalShuffle{
				Journal:     aJournal,
				Coordinator: "shard/2",
				Shuffle:     shuffles[0],
				Replay:      true,
				BuildId:     "build-id",
			},
			Range:     ranges,
			Offset:    1000,
			EndOffset: 2000,
		},
		resp:      pf.IndexedShuffleResponse{Shuffle: shuffles[0]},
		readDelay: 0,
	}, r)

	// Case: attempt to replay an unmatched journal.
	_, err = rb.buildReplayRead("not/matched", 1000, 2000)
	require.EqualError(t, err, "journal not matched for replay: not/matched")

	// Case: if the configuration changes, the existing *read
	// is drained so that it may be restarted.
	// This is a functional test of the implementation details:
	// ShuffleSpecs of a ReadBuilder are fixed and don't change dynamically.
	var copied = *shuffles[0]
	copied.ReadDelaySeconds++
	rb.shuffles[0] = &copied

	added, drain, err = rb.buildReads(existing, nil)
	require.NoError(t, err)
	require.Equal(t, []string{aJournal}, toKeys(drain))
	require.Empty(t, added)

	rb.shuffles[0] = shuffles[0] // Reset.

	// Case: if membership changes, we'll add and drain *reads as needed.
	rb.journals.KeyValues, rb.shuffles = allJournals.KeyValues[1:], shuffles
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
	existing = added

	// Begin to drain the ReadBuilder.
	close(drainCh)

	// Expect all reads now drain.
	added, drain, err = rb.buildReads(existing, nil)
	require.NoError(t, err)
	require.Equal(t, []string(nil), toKeys(added))
	require.Equal(t, []string{
		"foo/bar=1/baz=abc/part=01;transform/der/bar-one",
		"foo/bar=1/baz=def/part=00;transform/der/bar-one",
		"foo/bar=1/baz=def/part=00;transform/der/baz-def",
		"foo/bar=2/baz=def/part=00;transform/der/baz-def",
		"foo/bar=2/baz=def/part=01;transform/der/baz-def",
	}, toKeys(drain))

	// Draining doesn't invalidate replay reads.
	r, err = rb.buildReplayRead("foo/bar=1/baz=abc/part=01;transform/der/bar-one", 1000, 2000)
	require.NoError(t, err)
	require.NotNil(t, r)

	// It also doesn't invalidate offset filtering.
	offsets, err = rb.ReadThrough(pb.Offsets{
		"foo/bar=1/baz=def/part=00;transform/der/baz-def": 12,
		"foo/bar=1/baz=def/part=00":                       78,
	})
	require.NoError(t, err)
	require.Equal(t, offsets, pb.Offsets{
		"foo/bar=1/baz=def/part=00;transform/der/baz-def": 12,
		"foo/bar=1/baz=def/part=00;transform/der/bar-one": 78,
	})
}

func TestReadIteration(t *testing.T) {
	var r = &read{
		publisher: localPublisher,
		spec: pb.JournalSpec{
			Name: "a/journal",
		},
		resp: pf.IndexedShuffleResponse{
			Index: 0,
			ShuffleResponse: pf.ShuffleResponse{
				Offsets: []pb.Offset{0, 100, 200, 300, 400, 500},
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

	require.Equal(t, 2*r.resp.Index, len(r.resp.Offsets))
}

func TestReadHeaping(t *testing.T) {
	var resp = pf.ShuffleResponse{
		UuidParts: []pf.UUIDParts{
			{Clock: 2000},
			{Clock: 1001},
			{Clock: 1002},
			{Clock: 2003},
			{Clock: 1004},
			{Clock: 1005},
			{Clock: 1},
			{Clock: 2},
		},
	}
	var h readHeap

	// |p1| reads have earlier clocks, which would ordinarily be preferred,
	// but are withheld due to their lower priority.
	var p1 = pf.ShuffleRequest{
		Shuffle: pf.JournalShuffle{
			Shuffle: &pf.Shuffle{
				Priority: 1,
			},
		},
	}
	// |p2| reads have later clocks but are read first due to their higher priority.
	var p2 = pf.ShuffleRequest{
		Shuffle: pf.JournalShuffle{
			Shuffle: &pf.Shuffle{
				Priority: 2,
			},
		},
	}

	// Push reads in a mixed order.
	for _, r := range []*read{
		{req: p2, resp: pf.IndexedShuffleResponse{Index: 3, ShuffleResponse: resp}, readDelay: 1000},
		{req: p1, resp: pf.IndexedShuffleResponse{Index: 7, ShuffleResponse: resp}, readDelay: 0},
		{req: p2, resp: pf.IndexedShuffleResponse{Index: 1, ShuffleResponse: resp}, readDelay: 2000},
		{req: p2, resp: pf.IndexedShuffleResponse{Index: 0, ShuffleResponse: resp}, readDelay: 1000},
		{req: p1, resp: pf.IndexedShuffleResponse{Index: 6, ShuffleResponse: resp}, readDelay: 0},
		{req: p2, resp: pf.IndexedShuffleResponse{Index: 5, ShuffleResponse: resp}, readDelay: 2000},
		{req: p2, resp: pf.IndexedShuffleResponse{Index: 2, ShuffleResponse: resp}, readDelay: 2000},
		{req: p2, resp: pf.IndexedShuffleResponse{Index: 4, ShuffleResponse: resp}, readDelay: 2000},
	} {
		heap.Push(&h, r)
	}

	// Expect to pop reads in Index order, after adjusting for priority & readDelay.
	for ind := 0; ind != 8; ind++ {
		require.Equal(t, ind, heap.Pop(&h).(*read).resp.Index)
	}
	require.Empty(t, h)
}

func TestReadSendBackoffAndCancel(t *testing.T) {
	const capacity = 4
	var r = &read{
		publisher: localPublisher,
		ch:        make(chan *pf.ShuffleResponse, capacity),
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	var wakeCh = make(chan struct{}, 1)

	// If channel is regularly drained, sending is fast.
	for i := 0; i != 20; i++ {
		require.NoError(t, r.sendReadResult(new(pf.ShuffleResponse), nil, wakeCh))
		_, _ = <-r.ch, <-wakeCh // Both select.
	}
	// If channel is not drained, we can queue up to the channel capacity.
	for i := 0; i != capacity; i++ {
		require.NoError(t, r.sendReadResult(new(pf.ShuffleResponse), nil, wakeCh))
	}

	// An attempt to send more cancels the context.
	require.Equal(t, context.Canceled,
		r.sendReadResult(new(pf.ShuffleResponse), nil, wakeCh))

	// Attempt to send again, which mimics a context that was cancelled elsewhere.
	// Expect the cancellation aborts the send's exponential backoff timer.
	<-r.ch // No longer full.
	require.Equal(t, context.Canceled,
		r.sendReadResult(new(pf.ShuffleResponse), nil, wakeCh))

	<-wakeCh                        // Now empty.
	r.ch <- new(pf.ShuffleResponse) // Full again.

	// Send an error. Expect it sets |chErr|, closes the channel, and wakes |wakeCh|.
	// This must work despite the channel being at capacity (issue #226).
	require.Nil(t, r.chErr)
	require.NoError(t, r.sendReadResult(nil, io.ErrUnexpectedEOF, wakeCh))

	// Expect to read |capacity| messages, and then a close with the sent error.
	var count int
	var err error
	for err == nil {
		var rr, ok = <-r.ch
		if err = r.onRead(rr, ok); err == nil {
			count++
		}
	}
	require.Equal(t, capacity, count)
	require.Equal(t, io.ErrUnexpectedEOF, r.chErr)

	<-wakeCh // Was signaled.
}

func TestReadSendBackoffAndWake(t *testing.T) {
	const capacity = 24 // Very long backoff interval.
	var r = &read{
		ctx:       context.Background(),
		ch:        make(chan *pf.ShuffleResponse, capacity),
		drainedCh: make(chan struct{}, 1),
	}

	for i := 0; i != cap(r.ch)-1; i++ {
		r.ch <- new(pf.ShuffleResponse)
	}

	time.AfterFunc(time.Millisecond, func() {
		for i := 0; i != cap(r.ch)-1; i++ {
			<-r.ch // Empty it.
		}
		r.drainedCh <- struct{}{} // Notify of being emptied.
	})

	// A send starts a very long backoff timer, which is cancelled by
	// the above routine draining the channel while we're waiting.
	require.NoError(t, r.sendReadResult(new(pf.ShuffleResponse), nil, nil))

	<-r.ch // Blocked response was sent.
	require.Empty(t, r.ch)
}

func TestWalkingReads(t *testing.T) {
	var journals, shards, task = buildReadTestJournalsAndTransforms()
	var shuffles = task.TaskShuffles()

	// Expect coordinators align with physical partitions of logical groups.
	var expect = []struct {
		journal     string
		source      string
		coordinator pc.ShardID
	}{
		{"foo/bar=1/baz=abc/part=00;transform/der/bar-one", "foo", "shard/2"}, // Honors journal range.
		{"foo/bar=1/baz=abc/part=01;transform/der/bar-one", "foo", "shard/1"}, // Honors journal range.
		{"foo/bar=1/baz=def/part=00;transform/der/bar-one", "foo", "shard/0"}, // Honors journal range.
		{"foo/bar=1/baz=def/part=00;transform/der/baz-def", "foo", "shard/0"}, // Ignores journal range.
		{"foo/bar=2/baz=def/part=00;transform/der/baz-def", "foo", "shard/1"}, // Ignores journal range.
		{"foo/bar=2/baz=def/part=01;transform/der/baz-def", "foo", "shard/2"}, // Ignores journal range.
	}
	var err = walkReads(shards[0].Id, shards, journals, shuffles,
		func(_ pf.RangeSpec, spec pb.JournalSpec, shuffle *pf.Shuffle, coordinator pc.ShardID) {
			require.Equal(t, expect[0].journal, spec.Name.String())
			require.Equal(t, expect[0].source, shuffle.SourceCollection.String())
			require.Equal(t, expect[0].coordinator, coordinator)
			expect = expect[1:]
		})
	require.NoError(t, err)
	require.Empty(t, expect)

	// Walk with shard/0 and shard/1 only, such that the 0xcccccccc to 0xffffffff
	// portion of the key range is not covered by any shard.
	// This results in an error when walking with shuffle "bar-one" which uses the source key.
	err = walkReads(shards[0].Id, shards[0:2], journals, shuffles[:1],
		func(_ pf.RangeSpec, _ pb.JournalSpec, _ *pf.Shuffle, _ pc.ShardID) {})
	require.EqualError(t, err,
		"none of 2 shards overlap the key-range of journal foo/bar=1/baz=abc/part=00, cccccccc-ffffffff")
	// But is not an error with shuffle "baz-def", which *doesn't* use the source key.
	err = walkReads(shards[0].Id, shards[0:2], journals, shuffles[1:2],
		func(_ pf.RangeSpec, _ pb.JournalSpec, _ *pf.Shuffle, _ pc.ShardID) {})
	require.NoError(t, err)

	// Case: shard doesn't exist.
	err = walkReads("shard/deleted", shards, journals, shuffles,
		func(_ pf.RangeSpec, _ pb.JournalSpec, _ *pf.Shuffle, _ pc.ShardID) {})
	require.EqualError(t, err, "shard shard/deleted not found among shuffle members")
}

func TestHRWRegression(t *testing.T) {
	var ring = []shuffleMember{
		{hrwHash: 0xc7e1677},
		{hrwHash: 0xdbbc7dba},
		{hrwHash: 0xcbb36a2e},
		{hrwHash: 0x5389fa17},
	}
	var h uint32 = 0x2ffcbe05

	require.Equal(t, 1, pickHRW(h, ring, 0, 4))
	require.Equal(t, 0, pickHRW(h, ring, 0, 1))
	require.Equal(t, 1, pickHRW(h, ring, 1, 4))
	require.Equal(t, 2, pickHRW(h, ring, 2, 4))
	require.Equal(t, 2, pickHRW(h, ring, 2, 3))
}

func TestShuffleMemberOrdering(t *testing.T) {
	var _, shards, _ = buildReadTestJournalsAndTransforms()

	var members, err = newShuffleMembers(shards)
	require.NoError(t, err)

	// Test rangeSpan cases.
	for _, tc := range []struct {
		begin, end  uint32
		start, stop int
	}{
		// Exact matches of ranges.
		{0xaaaaaaaa, 0xbbbbbbba, 0, 1},
		{0xbbbbbbbb, 0xffffffff, 1, 3},
		// Partial overlap of single entry at list begin & end.
		{0xa0000000, 0xb0000000, 0, 1},
		{0xeeeeeeee, 0xffffffff, 2, 3},
		// Overlaps of multiple entries.
		{0x00000000, 0xc0000000, 0, 2},
		{0xc0000000, 0xd0000000, 1, 3},
	} {
		var start, stop = rangeSpan(members, tc.begin, tc.end)
		require.Equal(t, tc.start, start)
		require.Equal(t, tc.stop, stop)
	}

	// Add an extra shard which is not strictly greater than it's left-hand sibling.
	var withSplit = append(shards, &pc.ShardSpec{
		Id: "shard/3", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "cccccccc",
			labels.KeyEnd, "ffffffff",
			// RClock range overlaps with left sibling.
			labels.RClockBegin, "11111111",
			labels.RClockEnd, "99999999",
		)},
	)
	_, err = newShuffleMembers(withSplit)
	require.EqualError(t, err,
		"shard shard/3 range key:cccccccc-ffffffff;r-clock:11111111-99999999 is not "+
			"less-than shard shard/2 range key:cccccccc-ffffffff;r-clock:00000000-88888888")

	// Now add a split-source label. Expect the shard is ignored.
	withSplit[len(withSplit)-1].LabelSet.AddValue(labels.SplitSource, "foobar")
	members2, err := newShuffleMembers(withSplit)
	require.NoError(t, err)
	require.Len(t, members2, len(members))

	// Add an extra shard which doesn't have a valid RangeSpec.
	_, err = newShuffleMembers(append(shards, &pc.ShardSpec{
		Id: "shard/3", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "whoops",
		)},
	))
	require.EqualError(t, err,
		"shard shard/3: expected estuary.dev/key-begin to be a 4-byte, hex encoded integer; got whoops")
}

func buildReadTestJournalsAndTransforms() (flow.Journals, []*pc.ShardSpec, *pf.DerivationSpec) {
	var journals = flow.Journals{
		KeySpace: &keyspace.KeySpace{Root: "/the/root"}}

	for _, j := range []struct {
		bar   string
		baz   string
		begin string
		end   string
		part  int
	}{
		{"1", "abc", "cccccccc", "ffffffff", 0}, // foo/bar=1/baz=abc/part=00
		{"1", "abc", "bbbbbbbb", "cccccccb", 1}, // foo/bar=1/baz=abc/part=01
		{"1", "def", "aaaaaaaa", "bbbbbbba", 0}, // foo/bar=1/baz=def/part=00
		{"2", "def", "aaaaaaaa", "bbbbbbba", 0}, // foo/bar=2/baz=def/part=00
		{"2", "def", "bbbbbbbb", "ffffffff", 1}, // foo/bar=2/baz=def/part=01
	} {
		var name = fmt.Sprintf("foo/bar=%s/baz=%s/part=%02d", j.bar, j.baz, j.part)

		journals.KeyValues = append(journals.KeyValues, keyspace.KeyValue{
			Raw: mvccpb.KeyValue{
				Key: []byte(allocator.ItemKey(journals.KeySpace, name)),
			},
			Decoded: allocator.Item{ItemValue: &pb.JournalSpec{
				Name: pb.Journal(name),
				LabelSet: pb.MustLabelSet(
					labels.Collection, "foo",
					labels.FieldPrefix+"bar", j.bar,
					labels.FieldPrefix+"baz", j.baz,
					labels.KeyBegin, j.begin,
					labels.KeyEnd, j.end,
				),
			}},
		})
	}
	var shards = []*pc.ShardSpec{
		{Id: "shard/0", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "aaaaaaaa",
			labels.KeyEnd, "bbbbbbba",
			labels.RClockBegin, "00000000",
			labels.RClockEnd, "ffffffff")},
		{Id: "shard/1", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "bbbbbbbb",
			labels.KeyEnd, "cccccccb",
			labels.RClockBegin, "00000000",
			labels.RClockEnd, "ffffffff")},
		{Id: "shard/2", LabelSet: pb.MustLabelSet(
			labels.KeyBegin, "cccccccc",
			labels.KeyEnd, "ffffffff",
			labels.RClockBegin, "00000000",
			labels.RClockEnd, "88888888")},
	}

	// Derivation fixture reading partitions of "foo" into derivation "der".
	var task = &pf.DerivationSpec{
		Transforms: []pf.TransformSpec{
			{
				Transform: "bar-one",
				Shuffle: pf.Shuffle{
					GroupName:        "transform/der/bar-one",
					UsesSourceKey:    true,
					ReadDelaySeconds: 60,
					SourceCollection: "foo",
					SourcePartitions: pb.LabelSelector{
						Include: pb.MustLabelSet(labels.FieldPrefix+"bar", "1"),
					},
				},
				Derivation: "der",
			},
			{
				Transform: "baz-def",
				Shuffle: pf.Shuffle{
					GroupName:        "transform/der/baz-def",
					UsesSourceKey:    false,
					SourceCollection: "foo",
					SourcePartitions: pb.LabelSelector{
						Include: pb.MustLabelSet(labels.FieldPrefix+"baz", "def"),
					},
				},
				Derivation: "der",
			},
			{
				Transform: "unmatched",
				Shuffle: pf.Shuffle{
					GroupName:        "transform/der/unmatched",
					SourceCollection: "foo",
					SourcePartitions: pb.LabelSelector{
						Include: pb.MustLabelSet(labels.FieldPrefix+"baz", "other-value"),
					},
				},
				Derivation: "der",
			},
		},
	}
	return journals, shards, task
}
