package gazctlcmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
)

func TestFragmentsAreRetainedWhenAnyHintsRequireThem(t *testing.T) {
	// The goal is to allow a shard to recover using any of the persisted
	// hints. In order for that to work, all fragments that come after the final
	// hinted segment of _any_ hints must be retained. In this case, the primary
	// hints are quite old. So we test a fragment that would otherwise be elidgable
	// for pruning, and assert that it is retained due to coming after the segment
	// in the primary hints.
	var fixture = `{
    "header": {
      "process_id": {
        "zone": "us-central1-c",
        "suffix": "flow-reactor-74fcd7f9db-n5c5j"
      },
      "route": {
        "members": null,
        "primary": -1
      },
      "etcd": {
        "cluster_id": 9261053468663690000,
        "member_id": 14358579373743376000,
        "revision": 57121751,
        "raft_term": 140
      }
    },
    "primary_hints": {
      "hints": {
        "log": "example/log",
        "live_nodes": [
          {
            "fnode": 1018,
            "segments": [
              {
                "author": 3418428788,
                "first_seq_no": 1018,
                "first_offset": 129100,
                "first_checksum": 2628272215,
                "last_seq_no": 1024
              }
            ]
          }
        ],
        "properties": null
      }
    },
    "backup_hints": [
      {
        "hints": {
          "log": "example/log",
          "live_nodes": [
            {
              "fnode": 1109,
              "segments": [
                {
                  "author": 3418428788,
                  "first_seq_no": 1109,
                  "first_offset": 140706,
                  "first_checksum": 1095500179,
                  "last_seq_no": 1115,
                  "last_offset": 141535
                }
              ]
            },
            {
              "fnode": 1289,
              "segments": [
                {
                  "author": 2080950356,
                  "first_seq_no": 1289,
                  "first_offset": 152255,
                  "first_checksum": 777889612,
                  "last_seq_no": 1290,
                  "last_offset": 153297
                }
              ]
            },
            {
              "fnode": 1642,
              "segments": [
                {
                  "author": 245017003,
                  "first_seq_no": 1642,
                  "first_offset": 245137,
                  "first_checksum": 3605342194,
                  "last_seq_no": 1645,
                  "last_offset": 251592
                }
              ]
            },
            {
              "fnode": 1657,
              "segments": [
                {
                  "author": 3711033845,
                  "first_seq_no": 1657,
                  "first_offset": 252114,
                  "first_checksum": 794580610,
                  "last_seq_no": 1660,
                  "last_offset": 252379
                }
              ]
            },
            {
              "fnode": 1661,
              "segments": [
                {
                  "author": 3711033845,
                  "first_seq_no": 1661,
                  "first_offset": 252379,
                  "first_checksum": 157414297,
                  "last_seq_no": 1665,
                  "last_offset": 252574
                }
              ]
            },
            {
              "fnode": 1666,
              "segments": [
                {
                  "author": 3711033845,
                  "first_seq_no": 1666,
                  "first_offset": 252574,
                  "first_checksum": 157986885,
                  "last_seq_no": 1666,
                  "last_offset": 252610
                }
              ]
            },
            {
              "fnode": 1670,
              "segments": [
                {
                  "author": 3711033845,
                  "first_seq_no": 1670,
                  "first_offset": 252737,
                  "first_checksum": 1747582972,
                  "last_seq_no": 1673,
                  "last_offset": 259192
                }
              ]
            }
          ],
          "properties": [
            {
              "path": "/IDENTITY",
              "content": "0a4d4620-a93f-468a-9c12-26de543261f0\n"
            }
          ]
        }
      },
      {
        "hints": {
          "log": "example/log",
          "live_nodes": [
            {
              "fnode": 1109,
              "segments": [
                {
                  "author": 3418428788,
                  "first_seq_no": 1109,
                  "first_offset": 140706,
                  "first_checksum": 1095500179,
                  "last_seq_no": 1115,
                  "last_offset": 141535
                }
              ]
            },
            {
              "fnode": 1289,
              "segments": [
                {
                  "author": 2080950356,
                  "first_seq_no": 1289,
                  "first_offset": 152255,
                  "first_checksum": 777889612,
                  "last_seq_no": 1290,
                  "last_offset": 153297
                }
              ]
            },
            {
              "fnode": 1614,
              "segments": [
                {
                  "author": 1701696287,
                  "first_seq_no": 1614,
                  "first_offset": 237537,
                  "first_checksum": 3210869242,
                  "last_seq_no": 1617,
                  "last_offset": 243992
                }
              ]
            },
            {
              "fnode": 1629,
              "segments": [
                {
                  "author": 245017003,
                  "first_seq_no": 1629,
                  "first_offset": 244514,
                  "first_checksum": 3153286517,
                  "last_seq_no": 1632,
                  "last_offset": 244779
                }
              ]
            },
            {
              "fnode": 1633,
              "segments": [
                {
                  "author": 245017003,
                  "first_seq_no": 1633,
                  "first_offset": 244779,
                  "first_checksum": 3701750198,
                  "last_seq_no": 1637,
                  "last_offset": 244974
                }
              ]
            },
            {
              "fnode": 1638,
              "segments": [
                {
                  "author": 245017003,
                  "first_seq_no": 1638,
                  "first_offset": 244974,
                  "first_checksum": 2949463732,
                  "last_seq_no": 1638,
                  "last_offset": 245010
                }
              ]
            },
            {
              "fnode": 1642,
              "segments": [
                {
                  "author": 245017003,
                  "first_seq_no": 1642,
                  "first_offset": 245137,
                  "first_checksum": 3605342194,
                  "last_seq_no": 1645,
                  "last_offset": 251592
                }
              ]
            }
          ],
          "properties": [
            {
              "path": "/IDENTITY",
              "content": "0a4d4620-a93f-468a-9c12-26de543261f0\n"
            }
          ]
        }
      }
    ]
  }`

	var resp pc.GetHintsResponse
	require.NoError(t, json.Unmarshal([]byte(fixture), &resp))

	var set = make(map[pb.Journal][]recoverylog.Segment)

	for _, hints := range resp.BackupHints {
		foldHintsIntoSegments(*hints.Hints, set)
	}
	// Assert that the fragment would be pruned if we only accounted for the backup hints
	require.False(t, overlapsAnySegment(set["example/log"], pb.Fragment{
		Journal: "example/log",
		Begin:   243993,
		End:     244000,
	}))

	// Now add the primary hints and assert that the fragment would be retained
	foldHintsIntoSegments(*resp.PrimaryHints.Hints, set)
	require.True(t, overlapsAnySegment(set["example/log"], pb.Fragment{
		Journal: "example/log",
		Begin:   243993,
		End:     244000,
	}))
}

func TestSegmentFoldingWithSingleLog(t *testing.T) {
	var m = make(map[pb.Journal][]recoverylog.Segment)

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
	require.Equal(t, map[pb.Journal][]recoverylog.Segment{
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
	require.Equal(t, map[pb.Journal][]recoverylog.Segment{
		"a/log": {
			{Author: 0x1, FirstSeqNo: 2, FirstOffset: 200, LastSeqNo: 9, LastOffset: 901, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 25, FirstOffset: 2500, LastSeqNo: 27, LastOffset: 0, Log: "a/log"},
			{Author: 0x1, FirstSeqNo: 10, FirstOffset: 1000, LastSeqNo: 12, LastOffset: 1201, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 15, FirstOffset: 1500, LastSeqNo: 16, LastOffset: 1601, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 20, FirstOffset: 2000, LastSeqNo: 20, LastOffset: 0, Log: "a/log"}},
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
	require.Equal(t, map[pb.Journal][]recoverylog.Segment{
		"a/log": {
			{Author: 0x1, FirstSeqNo: 2, FirstOffset: 200, LastSeqNo: 9, LastOffset: 901, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 25, FirstOffset: 2500, LastSeqNo: 27, LastOffset: 0, Log: "a/log"},
			{Author: 0x1, FirstSeqNo: 10, FirstOffset: 1000, LastSeqNo: 12, LastOffset: 1201, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 15, FirstOffset: 1500, LastSeqNo: 16, LastOffset: 1601, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 20, FirstOffset: 2000, LastSeqNo: 20, LastOffset: 0, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 14, FirstOffset: 1400, LastSeqNo: 15, LastOffset: 1500, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 25, FirstOffset: 2500, LastSeqNo: 27, LastOffset: 2700, Log: "a/log"},
			{Author: 0x2, FirstSeqNo: 34, FirstOffset: 3400, LastSeqNo: 36, LastOffset: 0, Log: "a/log"},
		},
	}, m)

	require.False(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 0, End: 200}))
	require.True(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 0, End: 300}))
	require.True(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 1200, End: 1300}))
	require.False(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 1201, End: 1400}))
	require.False(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 1601, End: 2000}))

	// Any range intersecting the LastOffset==0 tail of the set is always overlapping (and not pruned).
	require.True(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 1601, End: 2001}))
	require.True(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 2100, End: 2200}))
	require.True(t, overlapsAnySegment(m["a/log"], pb.Fragment{Begin: 9998, End: 9999}))
}

func TestOverlapsAnySegment(t *testing.T) {
	var segments = []recoverylog.Segment{
		{FirstOffset: 50, LastOffset: 60},
		{FirstOffset: 70, LastOffset: 80},
		{FirstOffset: 90, LastOffset: 99},
	}
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 0, End: 51}))
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 0, End: 999}))
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 98, End: 0}))
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 59, End: 60}))
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 65, End: 85}))

	require.False(t, overlapsAnySegment(segments, pb.Fragment{Begin: 60, End: 70}))
	require.False(t, overlapsAnySegment(segments, pb.Fragment{Begin: 0, End: 50}))
	require.False(t, overlapsAnySegment(segments, pb.Fragment{Begin: 100, End: 99999}))

	segments = append(segments, recoverylog.Segment{FirstOffset: 99, LastOffset: 0})
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 100, End: 99999}))
	require.True(t, overlapsAnySegment(segments, pb.Fragment{Begin: 85, End: 99999}))
}

func TestSegmentFoldingWithManyLogs(t *testing.T) {
	var m = make(map[pb.Journal][]recoverylog.Segment)

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

	for _, h := range fixtures {
		foldHintsIntoSegments(h, m)
	}

	require.Equal(t, map[pb.Journal][]recoverylog.Segment{
		"l/one": {
			{Author: 0x30, FirstSeqNo: 30, FirstOffset: 30, LastSeqNo: 31, LastOffset: 31, Log: "l/one"},
			{Author: 0x70, FirstSeqNo: 70, FirstOffset: 70, LastSeqNo: 71, LastOffset: 0, Log: "l/one"},
			{Author: 0x20, FirstSeqNo: 20, FirstOffset: 20, LastSeqNo: 21, LastOffset: 21, Log: "l/one"},
			{Author: 0x40, FirstSeqNo: 40, FirstOffset: 40, LastSeqNo: 41, LastOffset: 41, Log: "l/one"},
		},
		"l/three": {
			{Author: 0x80, FirstSeqNo: 80, FirstOffset: 80, LastSeqNo: 81, LastOffset: 0, Log: "l/three"},
		},
		"l/two": {
			{Author: 0x60, FirstSeqNo: 60, FirstOffset: 60, LastSeqNo: 61, LastOffset: 0, Log: "l/two"},
			{Author: 0x50, FirstSeqNo: 50, FirstOffset: 50, LastSeqNo: 51, LastOffset: 51, Log: "l/two"},
		},
	}, m)
}
