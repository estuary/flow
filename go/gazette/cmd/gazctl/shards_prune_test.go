package main

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

func TestSegmentFoldingWithSingleLog(t *testing.T) {
	var m = make(map[pb.Journal]recoverylog.SegmentSet)

	foldHintsIntoSegments(recoverylog.FSMHints{
		Log: "a/log",
		LiveNodes: []recoverylog.FnodeSegments{
			{Fnode: 2, Segments: []recoverylog.Segment{
				{Author: 0x1, FirstSeqNo: 2, LastSeqNo: 7, FirstOffset: 200, LastOffset: 700},
			}},
			{Fnode: 4, Segments: []recoverylog.Segment{
				{Author: 0x1, FirstSeqNo: 4, LastSeqNo: 9, FirstOffset: 400, LastOffset: 901},
			}},
			{Fnode: 25, Segments: []recoverylog.Segment{
				{Author: 0x2, FirstSeqNo: 25, LastSeqNo: 27, FirstOffset: 2500, LastOffset: 2701},
			}},
		},
	}, m)

	// Expect segments are merged and final LastOffset is zero'd.
	require.Equal(t, map[pb.Journal]recoverylog.SegmentSet{
		"a/log": {
			recoverylog.Segment{Author: 0x1, FirstSeqNo: 2, FirstOffset: 200, LastSeqNo: 9, LastOffset: 901, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 25, FirstOffset: 2500, LastSeqNo: 27, LastOffset: 0, Log: "a/log"},
		},
	}, m)

	// Another set of hints requires additional intermediate files, which are added to the set.
	// It also has an earlier final segment as compared to our first hints, which is zero'd and
	// sets an earlier log horizon through which we'll prune.
	foldHintsIntoSegments(recoverylog.FSMHints{
		Log: "a/log",
		LiveNodes: []recoverylog.FnodeSegments{
			{Fnode: 10, Segments: []recoverylog.Segment{
				{Author: 0x1, FirstSeqNo: 10, LastSeqNo: 12, FirstOffset: 1000, LastOffset: 1201},
			}},
			{Fnode: 15, Segments: []recoverylog.Segment{
				{Author: 0x2, FirstSeqNo: 15, LastSeqNo: 16, FirstOffset: 1500, LastOffset: 1601},
			}},
			{Fnode: 20, Segments: []recoverylog.Segment{
				{Author: 0x2, FirstSeqNo: 20, LastSeqNo: 20, FirstOffset: 2000, LastOffset: 2001},
			}},
		},
	}, m)

	// Note that SegmentSet allows only a strict suffix of Segment to have a zero LastOffset.
	require.Equal(t, map[pb.Journal]recoverylog.SegmentSet{
		"a/log": {
			recoverylog.Segment{Author: 0x1, FirstSeqNo: 2, FirstOffset: 200, LastSeqNo: 12, LastOffset: 1201, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 15, FirstOffset: 1500, LastSeqNo: 16, LastOffset: 1601, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 20, FirstOffset: 2000, LastSeqNo: 20, LastOffset: 0, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 25, FirstOffset: 2500, LastSeqNo: 27, LastOffset: 0, Log: "a/log"},
		},
	}, m)

	// Final hints have later final segments.
	foldHintsIntoSegments(recoverylog.FSMHints{
		Log: "a/log",
		LiveNodes: []recoverylog.FnodeSegments{
			// Merges into existing segment.
			{Fnode: 14, Segments: []recoverylog.Segment{
				{Author: 0x2, FirstSeqNo: 14, LastSeqNo: 15, FirstOffset: 1400, LastOffset: 1500},
			}},
			{Fnode: 25, Segments: []recoverylog.Segment{
				{Author: 0x2, FirstSeqNo: 25, LastSeqNo: 27, FirstOffset: 2500, LastOffset: 2700},
			}},
			{Fnode: 34, Segments: []recoverylog.Segment{
				{Author: 0x2, FirstSeqNo: 34, LastSeqNo: 36, FirstOffset: 3400, LastOffset: 3600},
			}},
		},
	}, m)

	// Note that SegmentSet allows only a strict suffix of Segment to have a zero LastOffset.
	require.Equal(t, map[pb.Journal]recoverylog.SegmentSet{
		"a/log": {
			recoverylog.Segment{Author: 0x1, FirstSeqNo: 2, FirstOffset: 200, LastSeqNo: 12, LastOffset: 1201, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 14, FirstOffset: 1400, LastSeqNo: 16, LastOffset: 1601, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 20, FirstOffset: 2000, LastSeqNo: 20, LastOffset: 0, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 25, FirstOffset: 2500, LastSeqNo: 27, LastOffset: 0, Log: "a/log"},
			recoverylog.Segment{Author: 0x2, FirstSeqNo: 34, FirstOffset: 3400, LastSeqNo: 36, LastOffset: 0, Log: "a/log"},
		},
	}, m)

	require.Empty(t, m["a/log"].Intersect("a/log", 0, 200)) // May be pruned.
	require.NotEmpty(t, m["a/log"].Intersect("a/log", 0, 300))
	require.NotEmpty(t, m["a/log"].Intersect("a/log", 1200, 1300))
	require.Empty(t, m["a/log"].Intersect("a/log", 1201, 1400))
	require.Empty(t, m["a/log"].Intersect("a/log", 1601, 2000))

	// Any range intersecting the LastOffset==0 tail of the set is always overlapping (and not pruned).
	require.NotEmpty(t, m["a/log"].Intersect("a/log", 1601, 2001))
	require.NotEmpty(t, m["a/log"].Intersect("a/log", 2100, 2200))
	require.NotEmpty(t, m["a/log"].Intersect("a/log", 9998, 9999))
}

func TestSegmentFoldingWithManyLogs(t *testing.T) {
	var m = make(map[pb.Journal]recoverylog.SegmentSet)

	var fixtures = []recoverylog.FSMHints{
		{
			Log: "l/one",
			LiveNodes: []recoverylog.FnodeSegments{
				{Fnode: 30, Segments: []recoverylog.Segment{
					{Author: 0x30, FirstSeqNo: 30, LastSeqNo: 31, FirstOffset: 30, LastOffset: 31},
				}},
				{Fnode: 70, Segments: []recoverylog.Segment{
					{Author: 0x70, FirstSeqNo: 70, LastSeqNo: 71, FirstOffset: 70, LastOffset: 71},
				}},
			},
		},
		{
			Log: "l/two",
			LiveNodes: []recoverylog.FnodeSegments{
				{Fnode: 20, Segments: []recoverylog.Segment{
					{Author: 0x20, FirstSeqNo: 20, LastSeqNo: 21, FirstOffset: 20, LastOffset: 21, Log: "l/one"},
				}},
				{Fnode: 60, Segments: []recoverylog.Segment{
					{Author: 0x60, FirstSeqNo: 60, LastSeqNo: 61, FirstOffset: 60, LastOffset: 61},
				}},
			},
		},
		{
			Log: "l/three",
			LiveNodes: []recoverylog.FnodeSegments{
				{Fnode: 40, Segments: []recoverylog.Segment{
					{Author: 0x40, FirstSeqNo: 40, LastSeqNo: 41, FirstOffset: 40, LastOffset: 41, Log: "l/one"},
				}},
				{Fnode: 50, Segments: []recoverylog.Segment{
					{Author: 0x50, FirstSeqNo: 50, LastSeqNo: 51, FirstOffset: 50, LastOffset: 51, Log: "l/two"},
				}},
				{Fnode: 80, Segments: []recoverylog.Segment{
					{Author: 0x80, FirstSeqNo: 80, LastSeqNo: 81, FirstOffset: 80, LastOffset: 81},
				}},
			},
		},
	}

	for _, h := range rand.Perm(len(fixtures)) {
		foldHintsIntoSegments(fixtures[h], m)
	}

	require.Equal(t, map[pb.Journal]recoverylog.SegmentSet{

		"l/one": {
			recoverylog.Segment{Author: 0x20, FirstSeqNo: 20, FirstOffset: 20, LastSeqNo: 21, LastOffset: 21, Log: "l/one"},
			recoverylog.Segment{Author: 0x30, FirstSeqNo: 30, FirstOffset: 30, LastSeqNo: 31, LastOffset: 31, Log: "l/one"},
			recoverylog.Segment{Author: 0x40, FirstSeqNo: 40, FirstOffset: 40, LastSeqNo: 41, LastOffset: 41, Log: "l/one"},
			recoverylog.Segment{Author: 0x70, FirstSeqNo: 70, FirstOffset: 70, LastSeqNo: 71, LastOffset: 0, Log: "l/one"},
		},
		"l/three": {
			recoverylog.Segment{Author: 0x80, FirstSeqNo: 80, FirstOffset: 80, LastSeqNo: 81, LastOffset: 0, Log: "l/three"},
		},
		"l/two": {
			recoverylog.Segment{Author: 0x50, FirstSeqNo: 50, FirstOffset: 50, LastSeqNo: 51, LastOffset: 51, Log: "l/two"},
			recoverylog.Segment{Author: 0x60, FirstSeqNo: 60, FirstOffset: 60, LastSeqNo: 61, LastOffset: 0, Log: "l/two"},
		},
	}, m)
}
