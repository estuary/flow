package journal

import (
	gc "github.com/go-check/check"
)

type TailSuite struct {
	updates chan Fragment
	tail    *Tail
}

func (s *TailSuite) SetUpTest(c *gc.C) {
	s.updates = make(chan Fragment)
	s.tail = NewTail("a/journal", s.updates).StartServingOps()
}

func (s *TailSuite) TearDownTest(c *gc.C) {
	if s.updates != nil {
		close(s.updates)
	}
	s.tail.Stop()
}

func (s *TailSuite) TestNonblockingSuccess(c *gc.C) {
	fragment := Fragment{Journal: "a/journal", Begin: 100, End: 200}
	s.updates <- fragment

	results := make(chan ReadResult)
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{
			Journal: "a/journal",
			Offset:  150,
		},
		Result: results})

	c.Check(<-results, gc.DeepEquals, ReadResult{
		Offset:    150,
		WriteHead: 200,
		Fragment:  fragment,
	})
}

func (s *TailSuite) TestNonblockingFailure(c *gc.C) {
	fragment := Fragment{Journal: "a/journal", Begin: 100, End: 200}
	s.updates <- fragment

	results := make(chan ReadResult)

	// Explicit Read at current write head.
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Offset: 200},
		Result:   results})
	c.Check(<-results, gc.DeepEquals, ReadResult{
		Error:     ErrNotYetAvailable,
		Offset:    200,
		WriteHead: 200,
	})
	// Read at current write head (implicit).
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Offset: -1},
		Result:   results})
	c.Check(<-results, gc.DeepEquals, ReadResult{
		Error:     ErrNotYetAvailable,
		Offset:    200,
		WriteHead: 200,
	})
	// Read of a previous, uncovered offset.
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Offset: 50},
		Result:   results})
	c.Check(<-results, gc.DeepEquals, ReadResult{
		Error:     ErrNotYetAvailable,
		Offset:    50,
		WriteHead: 200,
	})
}

func (s *TailSuite) TestBlockingRead(c *gc.C) {
	s.updates <- Fragment{Journal: "a/journal", Begin: 100, End: 200}

	results := make(chan ReadResult, 2)
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Blocking: true, Offset: 200},
		Result:   results})
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Blocking: true, Offset: 300},
		Result:   results})
	c.Check(len(results), gc.Equals, 0)

	fragment1 := Fragment{Journal: "a/journal", Begin: 300, End: 400}
	s.updates <- fragment1

	// Second read unblocks.
	c.Check(<-results, gc.DeepEquals, ReadResult{
		Offset:    300,
		WriteHead: 400,
		Fragment:  fragment1,
	})

	fragment2 := Fragment{Journal: "a/journal", Begin: 200, End: 300}
	s.updates <- fragment2

	// First read unblocks.
	c.Check(<-results, gc.DeepEquals, ReadResult{
		Offset:    200,
		WriteHead: 400,
		Fragment:  fragment2,
	})

	// Read at implicit write head.
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Blocking: true, Offset: -1},
		Result:   results})
	// Note: this call re-enters tail.loop(), causing the op to queue.
	c.Check(s.tail.EndOffset(), gc.Equals, int64(400))

	fragment3 := Fragment{Journal: "a/journal", Begin: 400, End: 500}
	s.updates <- fragment3

	c.Check(<-results, gc.DeepEquals, ReadResult{
		Offset:    400, // Updated to reflect write head at operation start.
		WriteHead: 500,
		Fragment:  fragment3,
	})
}

func (s *TailSuite) TestClosingUpdatesUnblocksReads(c *gc.C) {
	s.updates <- Fragment{Journal: "a/journal", Begin: 100, End: 200}

	results := make(chan ReadResult, 2)
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Blocking: true, Offset: 200},
		Result:   results})
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Blocking: true, Offset: 300},
		Result:   results})
	c.Check(len(results), gc.Equals, 0)

	close(s.updates)
	s.updates = nil

	// Both reads unblock.
	result := <-results
	c.Check(result, gc.DeepEquals, ReadResult{
		Error:     ErrNotYetAvailable,
		Offset:    200,
		WriteHead: 200,
	})
	result = <-results
	c.Check(result, gc.DeepEquals, ReadResult{
		Error:     ErrNotYetAvailable,
		Offset:    300,
		WriteHead: 200,
	})

	// Additional blocking reads immediately resolve.
	s.tail.Read(ReadOp{
		ReadArgs: ReadArgs{Journal: "a/journal", Blocking: true, Offset: 200},
		Result:   results})
	result = <-results
	c.Check(result, gc.DeepEquals, ReadResult{
		Error:     ErrNotYetAvailable,
		Offset:    200,
		WriteHead: 200,
	})
}

func (s *TailSuite) TestReadFromZeroSkipsToFirstAvailable(c *gc.C) {
	fragment := Fragment{Journal: "a/journal", Begin: 100, End: 200}
	s.updates <- fragment

	results := make(chan ReadResult)

	for _, block := range []bool{true, false} {
		s.tail.Read(ReadOp{
			ReadArgs: ReadArgs{Journal: "a/journal", Blocking: block},
			Result:   results})

		c.Check(<-results, gc.DeepEquals, ReadResult{
			Offset:    100,
			WriteHead: 200,
			Fragment:  fragment,
		})
	}
}

func (s *TailSuite) TestEndOffsetGenerator(c *gc.C) {
	c.Check(s.tail.EndOffset(), gc.Equals, int64(0))
	s.updates <- Fragment{Journal: "a/journal", Begin: 100, End: 200}
	c.Check(s.tail.EndOffset(), gc.Equals, int64(200))

	s.updates <- Fragment{Journal: "a/journal", Begin: 300, End: 400}
	c.Check(s.tail.EndOffset(), gc.Equals, int64(400))
	s.updates <- Fragment{Journal: "a/journal", Begin: 200, End: 300}
	c.Check(s.tail.EndOffset(), gc.Equals, int64(400))

	s.updates <- Fragment{Journal: "a/journal", Begin: 350, End: 450}
	s.updates <- Fragment{Journal: "a/journal", Begin: 375, End: 475}
	c.Check(s.tail.EndOffset(), gc.Equals, int64(475))
}

var _ = gc.Suite(&TailSuite{})
