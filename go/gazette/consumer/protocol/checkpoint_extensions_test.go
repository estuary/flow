package protocol

import (
	"sort"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

type CheckpointSuite struct{}

func (s *CheckpointSuite) TestRoundTrip(c *gc.C) {
	var fixture = BuildCheckpointArgs{
		ReadThrough: pb.Offsets{
			"foo": 111,
			"bar": 222,
		},
		ProducerStates: []message.ProducerState{
			{
				JournalProducer: message.JournalProducer{
					Journal:  "foo",
					Producer: message.ProducerID{1, 2, 3, 4, 5, 6},
				},
				LastAck: 456,
				Begin:   789,
			},
			{
				JournalProducer: message.JournalProducer{
					Journal:  "bar",
					Producer: message.ProducerID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
				},
				LastAck: 789,
				Begin:   101,
			},
		},
		AckIntents: []message.AckIntent{
			{
				Journal: "baz",
				Intent:  []byte("intent"),
			},
			{
				Journal: "bing",
				Intent:  []byte("other-intent"),
			},
		},
	}

	// Expect BuildCheckpoint re-combines into a Checkpoint message.
	var cp = BuildCheckpoint(fixture)
	c.Check(cp.AckIntents, gc.DeepEquals, map[pb.Journal][]byte{
		"baz":  []byte("intent"),
		"bing": []byte("other-intent"),
	})
	c.Check(cp.Sources, gc.DeepEquals, map[pb.Journal]*Checkpoint_Source{
		"foo": {
			ReadThrough: 111,
			Producers: []Checkpoint_Source_ProducerEntry{
				{
					Id: []byte{1, 2, 3, 4, 5, 6},
					State: Checkpoint_ProducerState{
						LastAck: 456,
						Begin:   789,
					},
				},
			},
		},
		"bar": {
			ReadThrough: 222,
			Producers: []Checkpoint_Source_ProducerEntry{
				{
					Id: []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
					State: Checkpoint_ProducerState{
						LastAck: 789,
						Begin:   101,
					},
				},
			},
		},
	})

	// Expect we recover our inputs.
	var out = FlattenProducerStates(cp)
	if out[0].Journal != "foo" {
		out[0], out[1] = out[1], out[0] // Order not preserved.
	}
	c.Check(out, gc.DeepEquals, fixture.ProducerStates)
	c.Check(FlattenReadThrough(cp), gc.DeepEquals, fixture.ReadThrough)

}

func (s *CheckpointSuite) TestProducerIDMapRegression(c *gc.C) {
	// This byte fixture was produced by a Checkpoint implementation which used
	// a protobuf map<string, ProducerState> to store producer states.
	//
	// This was an incorrect implementation, as it results in strings which are
	// not UTF-8 -- a violation of the protobuf spec. To correct for this,
	// Checkpoint was modified to use a repeated message of producer IDs and
	// states which maintain wire-compatibility with the previous encoding.
	//
	// To confirm this, we expect this fixture (produced by the old implementation)
	// to properly decode with the current one.
	var fixture = []byte{
		0xa, 0x21, 0xa, 0x3, 0x62, 0x61, 0x72, 0x12, 0x1a, 0x8, 0xde, 0x1, 0x12, 0x15, 0xa, 0x6,
		0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x12, 0xb, 0x9, 0x15, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x10, 0x65, 0xa, 0x21, 0xa, 0x3, 0x66, 0x6f, 0x6f, 0x12, 0x1a, 0x8, 0x6f, 0x12, 0x16,
		0xa, 0x6, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x12, 0xc, 0x9, 0xc8, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x10, 0x95, 0x6, 0x12, 0xd, 0xa, 0x3, 0x62, 0x61, 0x7a, 0x12, 0x6, 0x69, 0x6e, 0x74,
		0x65, 0x6e, 0x74, 0x12, 0x14, 0xa, 0x4, 0x62, 0x69, 0x6e, 0x67, 0x12, 0xc, 0x6f, 0x74, 0x68,
		0x65, 0x72, 0x2d, 0x69, 0x6e, 0x74, 0x65, 0x6e, 0x74,
	}
	var cp Checkpoint
	c.Check(cp.Unmarshal(fixture), gc.IsNil)

	var states = FlattenProducerStates(cp)
	sort.Slice(states, func(i, j int) bool { return states[i].JournalProducer.Journal < states[j].JournalProducer.Journal })

	c.Check(states, gc.DeepEquals, []message.ProducerState{
		{
			JournalProducer: message.JournalProducer{
				Journal:  "bar",
				Producer: message.ProducerID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			},
			LastAck: 789,
			Begin:   101,
		},
		{
			JournalProducer: message.JournalProducer{
				Journal:  "foo",
				Producer: message.ProducerID{1, 2, 3, 4, 5, 6},
			},
			LastAck: 456,
			Begin:   789,
		},
	})
	c.Check(FlattenReadThrough(cp), gc.DeepEquals,
		pb.Offsets{
			"foo": 111,
			"bar": 222,
		})

	c.Check(cp.AckIntents, gc.DeepEquals, map[pb.Journal][]byte{
		"baz":  []byte("intent"),
		"bing": []byte("other-intent"),
	})
}

var _ = gc.Suite(&CheckpointSuite{})
