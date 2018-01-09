package main

import (
	"testing"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/examples/stream-sum"
	"github.com/LiveRamp/gazette/pkg/consumer/consumertest"
	"github.com/LiveRamp/gazette/pkg/topic"
)

type SummerSuite struct{}

func (s *SummerSuite) TestSummingWithFixtures(c *gc.C) {
	var summer summer

	var shard, err = consumertest.NewShard("bang")
	c.Assert(err, gc.IsNil)
	c.Assert(summer.InitShard(shard), gc.IsNil)
	defer shard.Close()

	var mw = topic.NewMemoryWriter(stream_sum.Sums.Framing, stream_sum.Sums.GetMessage)
	var pub = topic.NewPublisher(mw)

	var transaction = func(chunks []stream_sum.Chunk) {
		for _, chunk := range chunks {
			c.Check(summer.Consume(topic.Envelope{Message: &chunk}, shard, pub), gc.IsNil)
		}
		c.Check(summer.Flush(shard, nil), gc.IsNil)
		c.Assert(shard.FlushTransaction(), gc.IsNil)
	}

	var id1, id2, id3 = stream_sum.NewStreamID(), stream_sum.NewStreamID(), stream_sum.NewStreamID()

	transaction([]stream_sum.Chunk{
		{ID: id1, SeqNo: 1, Data: []byte("battery ")},
		{ID: id2, SeqNo: 1, Data: []byte("battery horse ")},
		{ID: id3, SeqNo: 1, Data: []byte("woosh")},
	})

	c.Check(mw.Messages, gc.HasLen, 0)             // No sums published.
	c.Check(shard.DatabaseContent(), gc.HasLen, 3) // All partial sums persisted.

	transaction([]stream_sum.Chunk{
		{ID: id3, SeqNo: 1, Data: []byte("woosh")}, // Ignored repeat.
		{ID: id1, SeqNo: 2, Data: []byte("horse ")},
		{ID: id2, SeqNo: 2, Data: []byte("stapler")},
		{ID: id1, SeqNo: 2, Data: []byte("horse ")}, // Ignored repeat.
		{ID: id1, SeqNo: 3, Data: []byte("stap")},
		{ID: id3, SeqNo: 2, Data: nil}, // End-of-stream.
	})

	// Expect a final sum for |id3| was published.
	c.Check(mw.Messages[0].Message.(*stream_sum.Sum), gc.DeepEquals,
		&stream_sum.Sum{ID: id3, SeqNo: 2, Value: 0xc2edb670edd6c4a2})

	transaction([]stream_sum.Chunk{
		{ID: id1, SeqNo: 4, Data: []byte("ler")},
		{ID: id2, SeqNo: 3, Data: nil}, // End-of-stream.
		{ID: id1, SeqNo: 5, Data: nil}, // End-of-stream.
	})

	// Expect final matched sums for |id2| & |id3| were published.
	c.Check(mw.Messages[1].Message.(*stream_sum.Sum), gc.DeepEquals,
		&stream_sum.Sum{ID: id2, SeqNo: 3, Value: 0x29912ed897bcedd8})
	c.Check(mw.Messages[2].Message.(*stream_sum.Sum), gc.DeepEquals,
		&stream_sum.Sum{ID: id1, SeqNo: 5, Value: 0x29912ed897bcedd8})
}

var _ = gc.Suite(&SummerSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
