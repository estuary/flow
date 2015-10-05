package gazette

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
	s.tail.Read(ReadOp{Journal: "a/journal", Offset: 150, Result: results})
	result := <-results

	c.Check(result.Error, gc.IsNil)
	c.Check(result.Fragment, gc.DeepEquals, fragment)
}

func (s *TailSuite) TestNonblockingFailure(c *gc.C) {
	fragment := Fragment{Journal: "a/journal", Begin: 100, End: 200}
	s.updates <- fragment

	results := make(chan ReadResult)
	s.tail.Read(ReadOp{Journal: "a/journal", Offset: 200, Result: results})

	result := <-results
	c.Check(result.Error, gc.Equals, ErrNotYetAvailable)
}

func (s *TailSuite) TestBlockingRead(c *gc.C) {
	s.updates <- Fragment{Journal: "a/journal", Begin: 100, End: 200}

	results := make(chan ReadResult, 2)
	s.tail.Read(ReadOp{Journal: "a/journal", Blocking: true, Offset: 200,
		Result: results})
	s.tail.Read(ReadOp{Journal: "a/journal", Blocking: true, Offset: 300,
		Result: results})
	c.Check(len(results), gc.Equals, 0)

	fragment1 := Fragment{Journal: "a/journal", Begin: 300, End: 400}
	s.updates <- fragment1

	// Second read unblocks.
	result := <-results
	c.Check(result.Error, gc.IsNil)
	c.Check(result.Fragment, gc.DeepEquals, fragment1)
	c.Check(len(results), gc.Equals, 0)

	fragment2 := Fragment{Journal: "a/journal", Begin: 200, End: 300}
	s.updates <- fragment2

	// First read unblocks.
	result = <-results
	c.Check(result.Error, gc.IsNil)
	c.Check(result.Fragment, gc.DeepEquals, fragment2)
}

func (s *TailSuite) TestClosingUpdatesUnblocksReads(c *gc.C) {
	s.updates <- Fragment{Journal: "a/journal", Begin: 100, End: 200}

	results := make(chan ReadResult, 2)
	s.tail.Read(ReadOp{Journal: "a/journal", Blocking: true, Offset: 200,
		Result: results})
	s.tail.Read(ReadOp{Journal: "a/journal", Blocking: true, Offset: 300,
		Result: results})
	c.Check(len(results), gc.Equals, 0)

	close(s.updates)
	s.updates = nil

	// Both reads unblock.
	result := <-results
	c.Check(result.Error, gc.Equals, ErrNotYetAvailable)
	result = <-results
	c.Check(result.Error, gc.Equals, ErrNotYetAvailable)

	// Additional blocking reads immediately resolve.
	s.tail.Read(ReadOp{Journal: "a/journal", Blocking: true, Offset: 200,
		Result: results})
	result = <-results
	c.Check(result.Error, gc.Equals, ErrNotYetAvailable)
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
