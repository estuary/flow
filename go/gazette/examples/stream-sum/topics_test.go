package stream_sum

import (
	"testing"

	"github.com/LiveRamp/gazette/pkg/journal"
	gc "github.com/go-check/check"
)

type TopicsSuite struct{}

func (s *TopicsSuite) TestUpdateSumCases(c *gc.C) {
	var id = NewStreamID()

	for _, tc := range []struct {
		Chunk
		in     Sum
		expect Sum
	}{
		{Chunk{SeqNo: 1, Data: []byte("battery horse")}, Sum{SeqNo: 0, Value: 0x0},
			Sum{SeqNo: 1, Value: 0xae763109c1192ed6}},

		// Expect variations of chunking still produce the same final sum.
		{Chunk{SeqNo: 1, Data: []byte("battery")}, Sum{SeqNo: 0, Value: 0x0},
			Sum{SeqNo: 1, Value: 0x2be55fc66a381c84}},
		{Chunk{SeqNo: 2, Data: []byte(" horse")}, Sum{SeqNo: 1, Value: 0x2be55fc66a381c84},
			Sum{SeqNo: 2, Value: 0xae763109c1192ed6}},

		// Replays of earlier SeqNo don't update the sum.
		{Chunk{SeqNo: 1, Data: []byte("battery")}, Sum{SeqNo: 2, Value: 0xae763109c1192ed6},
			Sum{SeqNo: 2, Value: 0xae763109c1192ed6}},
		{Chunk{SeqNo: 2, Data: []byte(" horse")}, Sum{SeqNo: 2, Value: 0xae763109c1192ed6},
			Sum{SeqNo: 2, Value: 0xae763109c1192ed6}},
	} {
		tc.Chunk.ID, tc.in.ID, tc.expect.ID = id, id, id

		var done, err = tc.in.Update(tc.Chunk)
		c.Check(done, gc.Equals, false)
		c.Check(err, gc.IsNil)
		c.Check(tc.in, gc.Equals, tc.expect)
	}

	// Expect a skipped SeqNo produces an error.
	var sum = Sum{ID: id, SeqNo: 2, Value: 0xfeedbeef}
	var done, err = sum.Update(Chunk{ID: id, SeqNo: 4, Data: []byte("foo")})
	c.Check(err, gc.ErrorMatches, "invalid chunk.SeqNo .*")

	// Expect a mismatched ID produces an error.
	_, err = sum.Update(Chunk{ID: NewStreamID(), SeqNo: 3, Data: []byte("foo")})
	c.Check(err, gc.ErrorMatches, "invalid chunk.ID .*")

	// A terminating (empty) Chunk returns |done|.
	done, err = sum.Update(Chunk{ID: id, SeqNo: 3, Data: nil})
	c.Check(err, gc.IsNil)
	c.Check(done, gc.Equals, true)
}

func (s *TopicsSuite) TestPartitionMappingWithFixtures(c *gc.C) {
	c.Check(Chunks.Partitions(), gc.HasLen, 8)
	c.Check(Sums.Partitions(), gc.HasLen, 1)

	var chunk = Chunks.GetMessage().(*Chunk)
	copy(chunk.ID[:], "feedbeeffeedbeef")

	// Expect Chunks are stably mapped on Chunk.ID.
	c.Check(Chunks.MappedPartition(chunk), gc.Equals, journal.Name("examples/stream-sum/chunks/part-005"))

	// Expect Sums map to a single journal.
	var sum = Sums.GetMessage().(*Sum)
	c.Check(Sums.MappedPartition(sum), gc.Equals, journal.Name("examples/stream-sum/sums"))
}

var _ = gc.Suite(&TopicsSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
