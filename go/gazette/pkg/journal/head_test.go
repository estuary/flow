package journal

import (
	"io/ioutil"
	"os"

	gc "github.com/go-check/check"
)

type HeadSuite struct {
	localDir string

	rolled, updates chan Fragment

	head *Head
}

func (s *HeadSuite) SetUpTest(c *gc.C) {
	var err error
	s.localDir, err = ioutil.TempDir("", "head-suite")
	c.Assert(err, gc.IsNil)

	s.rolled = make(chan Fragment, 1)
	s.updates = make(chan Fragment, 1)

	s.head = NewHead("a/journal", s.localDir, s, s.updates).
		StartServingOps(123456)
}

func (s *HeadSuite) Persist(fragment Fragment) {
	s.rolled <- fragment
}

func (s *HeadSuite) TearDownTest(c *gc.C) {
	s.head.Stop()
	if s.head.spool != nil {
		<-s.rolled // Expect it to be emitted.
	}
	os.RemoveAll(s.localDir)
}

func (s *HeadSuite) opFixture() ReplicateOp {
	return ReplicateOp{
		ReplicateArgs: ReplicateArgs{
			Journal:    "a/journal",
			RouteToken: "a-route-token",
			WriteHead:  123456,
			NewSpool:   false,
		},
		Result: make(chan ReplicateResult),
	}
}

func (s *HeadSuite) TestBasicWrite(c *gc.C) {
	var op = s.opFixture()
	s.head.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.IsNil)
	c.Check(result.Writer, gc.Not(gc.IsNil))

	result.Writer.Write([]byte("write body"))
	c.Check(result.Writer.Commit(10), gc.IsNil)
	c.Check(s.head.writeHead, gc.Equals, int64(123466))

	// Expect a committed fragment was written, backed by a local file.
	fragment := <-s.updates
	c.Check(fragment.Begin, gc.Equals, int64(123456))
	c.Check(fragment.End, gc.Equals, int64(123466))
	c.Check(fragment.File, gc.NotNil)
}

func (s *HeadSuite) TestNoWrite(c *gc.C) {
	var op = s.opFixture()
	s.head.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.IsNil)
	c.Check(result.Writer, gc.Not(gc.IsNil))

	result.Writer.Write([]byte("write body"))
	c.Check(result.Writer.Commit(0), gc.IsNil)
	c.Check(s.head.writeHead, gc.Equals, int64(123456))

	// A trivial (empty) fragment was written.
	fragment := <-s.updates
	c.Check(fragment.Begin, gc.Equals, int64(123456))
	c.Check(fragment.End, gc.Equals, int64(123456))
}

func (s *HeadSuite) TestSequentialWrite(c *gc.C) {
	s.TestBasicWrite(c)

	// Perform another sequenced write after the first.
	var op = s.opFixture()
	op.WriteHead = 123466
	s.head.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.IsNil)

	result.Writer.Write([]byte("another write body"))
	c.Check(result.Writer.Commit(18), gc.IsNil)
	c.Check(s.head.writeHead, gc.Equals, int64(123484))

	// A new fragment was produced from the same, current spool.
	fragment := <-s.updates
	c.Check(fragment.Begin, gc.Equals, int64(123456))
	c.Check(fragment.End, gc.Equals, int64(123484))
	c.Check(fragment.File, gc.NotNil)
}

func (s *HeadSuite) TestWriteHeadIncreaseHandling(c *gc.C) {
	s.TestBasicWrite(c)

	// Perform another write beginning beyond the current head.
	var op = s.opFixture()
	op.WriteHead = 234567
	s.head.Replicate(op)

	// The former spool was rolled.
	spool := <-s.rolled
	c.Check(spool.End, gc.Equals, int64(123466))

	result := <-op.Result
	c.Check(result.Error, gc.IsNil)

	result.Writer.Write([]byte("another write body"))
	c.Check(result.Writer.Commit(18), gc.IsNil)
	c.Check(s.head.writeHead, gc.Equals, int64(234585))

	// A new fragment was produced with a new spool.
	fragment := <-s.updates
	c.Check(fragment.Begin, gc.Equals, int64(234567))
	c.Check(fragment.End, gc.Equals, int64(234585))
	c.Check(fragment.File, gc.NotNil)
}

func (s *HeadSuite) TestSequentialWriteWithNewSpool(c *gc.C) {
	s.TestBasicWrite(c)

	// Perform another sequenced write after the first.
	var op = s.opFixture()
	op.WriteHead = 123466
	op.NewSpool = true
	s.head.Replicate(op)

	// The former spool was rolled.
	spool := <-s.rolled
	c.Check(spool.End, gc.Equals, int64(123466))

	result := <-op.Result
	c.Check(result.Error, gc.IsNil)

	result.Writer.Write([]byte("another write body"))
	c.Check(result.Writer.Commit(18), gc.IsNil)
	c.Check(s.head.writeHead, gc.Equals, int64(123484))

	// A new fragment was produced from the new spool.
	fragment := <-s.updates
	c.Check(fragment.Begin, gc.Equals, int64(123466))
	c.Check(fragment.End, gc.Equals, int64(123484))
	c.Check(fragment.File, gc.NotNil)
}

func (s *HeadSuite) TestIncorrectWriteOffsetHandling(c *gc.C) {
	var op = s.opFixture()
	op.WriteHead = 123
	s.head.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.ErrorMatches, "wrong write head")
	c.Check(result.ErrorWriteHead, gc.Equals, int64(123456))
	c.Check(result.Writer, gc.IsNil)
}

var _ = gc.Suite(&HeadSuite{})
