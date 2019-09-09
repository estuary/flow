package protocol

import (
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
					Producer: message.ProducerID{1, 2, 3},
				},
				LastAck: 456,
				Begin:   789,
			},
			{
				JournalProducer: message.JournalProducer{
					Journal:  "bar",
					Producer: message.ProducerID{4, 5, 6},
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
	c.Check(cp.Sources, gc.DeepEquals, map[pb.Journal]Checkpoint_Source{
		"foo": {
			ReadThrough: 111,
			Producers: map[string]Checkpoint_ProducerState{
				string([]byte{1, 2, 3, 0, 0, 0}): {
					LastAck: 456,
					Begin:   789,
				},
			},
		},
		"bar": {
			ReadThrough: 222,
			Producers: map[string]Checkpoint_ProducerState{
				string([]byte{4, 5, 6, 0, 0, 0}): {
					LastAck: 789,
					Begin:   101,
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

var _ = gc.Suite(&CheckpointSuite{})
