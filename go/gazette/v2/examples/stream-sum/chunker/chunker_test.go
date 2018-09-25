package main

import (
	"bufio"
	"io"
	"testing"

	"github.com/LiveRamp/gazette/v2/examples/stream-sum"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	gc "github.com/go-check/check"
)

type ChunkerSuite struct{}

func (s *ChunkerSuite) TestGenerationWithSimpleFixture(c *gc.C) {
	var chunkCh = make(chan stream_sum.Chunk)
	var verifyCh = make(chan stream_sum.Sum)

	go generate(1, 2, verifyCh, chunkCh)

	// Expect exactly three chunks are generated, with the 3rd being empty (reflecting end-of-stream),
	// and |chunkCh| is closed thereafter (terminating the range).
	var i int
	var verify stream_sum.Sum

	for chunk := range chunkCh {
		if i++; i == 1 {
			verify.ID = chunk.ID
		}

		c.Check(chunk.ID, gc.Equals, verify.ID)
		c.Check(chunk.SeqNo, gc.Equals, i)

		if i != 3 {
			c.Check(chunk.Data, gc.NotNil)
			continue
		}

		// EOF. Expect a sum is emitted for verification.
		c.Check(chunk.Data, gc.IsNil)
		verify = <-verifyCh

		c.Check(chunk.ID, gc.Equals, verify.ID)
		c.Check(chunk.SeqNo, gc.Equals, verify.SeqNo)

		// Expect |verifyCh| was closed.
		var _, ok = <-verifyCh
		c.Check(ok, gc.Equals, false)
	}
}

func (s *ChunkerSuite) TestGeneratePumpAndVerify(c *gc.C) {
	const nStreams, nChunks = 20, 20

	// Wire together each of |generate|, |verify|, and |pumpSums|. Use a tee and
	// io.Pipe to duplex sums for verification to |teeCh| and the |pumpSums| reader.
	var chunkCh = make(chan stream_sum.Chunk)
	var verifyCh = make(chan stream_sum.Sum)
	var teeCh = make(chan stream_sum.Sum)
	var actualCh = make(chan stream_sum.Sum)

	var pr, pw = io.Pipe()

	go func(in <-chan stream_sum.Sum, out chan<- stream_sum.Sum, w io.WriteCloser) {
		defer close(out)
		defer w.Close()

		var bw = bufio.NewWriter(w)

		for sum := range in {
			// Marshal |sum| into the io.Writer.
			c.Check(message.JSONFraming.Marshal(sum, bw), gc.IsNil)
			c.Check(bw.Flush(), gc.IsNil)
			out <- sum
		}

	}(verifyCh, teeCh, pw)

	go pumpSums(bufio.NewReader(pr), actualCh)
	go generate(nStreams, nChunks, verifyCh, chunkCh)

	var allStreams = make(map[stream_sum.StreamID]struct{})
	var allChunks int

	verify(func(chunk stream_sum.Chunk) {
		allStreams[chunk.ID] = struct{}{}
		allChunks++
	}, chunkCh, teeCh, actualCh)

	// Expect we saw |nStreams| streams each with |nChunks| chunks (plus 1 for EOF).
	c.Check(allStreams, gc.HasLen, nStreams)
	c.Check(allChunks, gc.Equals, nStreams*(nChunks+1))
}

var _ = gc.Suite(&ChunkerSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
