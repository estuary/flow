package journal

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"strings"
	"testing/iotest"

	gc "github.com/go-check/check"
	"github.com/stretchr/testify/mock"
)

type IOSuite struct{}

func (s *IOSuite) TestMarkedReaderUpdates(c *gc.C) {
	var reader = struct {
		io.Reader
		closeCh
	}{iotest.TimeoutReader(iotest.HalfReader(strings.NewReader("afixture"))), make(closeCh)}

	var mr = NewMarkedReader(Mark{Journal: "a/journal", Offset: 1234}, reader)
	var buffer [8]byte

	// First read: read succeeds at half of the request size.
	var n, err = mr.Read(buffer[:6])
	c.Check(n, gc.Equals, 3)
	c.Check(err, gc.IsNil)

	c.Check(mr.Mark.Journal, gc.Equals, Name("a/journal"))
	c.Check(mr.Mark.Offset, gc.Equals, int64(1237))

	// Second read: error is encountered & passed through.
	n, err = mr.Read(buffer[:6])
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, iotest.ErrTimeout)

	c.Check(mr.Mark.Offset, gc.Equals, int64(1237))

	c.Check(mr.Close(), gc.Equals, nil)
	<-reader.closeCh // Expect Close() to have been called.
}

func (s *IOSuite) TestBufferedMarkAdjustment(c *gc.C) {
	var reader = struct {
		io.Reader
		closeCh
	}{strings.NewReader("foobar\nbaz\n"), make(closeCh)}

	var mr = NewMarkedReader(Mark{Journal: "a/journal", Offset: 1234}, reader)
	var br = bufio.NewReader(mr)

	var b, err = br.ReadBytes('\n')
	c.Check(string(b), gc.Equals, "foobar\n")
	c.Check(err, gc.IsNil)

	// Expect the entire input reader was consumed.
	c.Check(mr.Mark.Offset, gc.Equals, int64(1234+7+4))
	// Expect the adjusted mark reflects just the portion read from |br|.
	c.Check(mr.AdjustedMark(br).Offset, gc.Equals, int64(1234+7))
}

func (s *IOSuite) TestReaderRetries(c *gc.C) {
	// Sequence of test readers which will be returned by sequential Get's.
	var readers = []struct {
		io.Reader
		closeCh
	}{
		{iotest.DataErrReader(iotest.OneByteReader(strings.NewReader("foo"))), make(closeCh)},
		{iotest.TimeoutReader(iotest.HalfReader(strings.NewReader("barbaXXX"))), make(closeCh)},
		{strings.NewReader(""), make(closeCh)},
		{iotest.HalfReader(strings.NewReader("zbingYYY")), make(closeCh)},
	}

	var ctx, cancel = context.WithCancel(context.Background())
	var getter = new(MockGetter)

	var rr = NewRetryReaderContext(ctx, Mark{"a/journal", -1}, getter)
	rr.Blocking = false

	// Initial read of 3 bytes, which increments the offset from 0 -> 100 and then EOFs.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: -1, Blocking: false, Context: ctx}).
		Return(ReadResult{Offset: 100}, readers[0]).Once()

	// Next open fails.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Blocking: false, Context: ctx}).
		Return(ReadResult{Error: ErrNotBroker}, ioutil.NopCloser(nil)).Once()

	// Read is retried, and the ErrNotYetAvailable is surfaced to the caller.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Blocking: false, Context: ctx}).
		Return(ReadResult{Error: ErrNotYetAvailable}, ioutil.NopCloser(nil)).Once()

	// Expect |rr| is switched to blocking. Next ErrNotYetAvailable is swallowed and retried.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Blocking: true, Context: ctx}).
		Return(ReadResult{Error: ErrNotYetAvailable}, ioutil.NopCloser(nil)).Once()

	// Next open jumps the offset, reads 5 bytes, then times out.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Blocking: true, Context: ctx}).
		Return(ReadResult{Offset: 203}, readers[1]).Once()

	// Next open returns a reader which EOFs without content.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 208, Blocking: true, Context: ctx}).
		Return(ReadResult{Offset: 208}, readers[2]).Once()

	// Next open reads remaining 5 bytes at the updated offset.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 208, Blocking: true, Context: ctx}).
		Return(ReadResult{Offset: 208}, readers[3]).Once()

	// All subsequent reads return an error.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 216, Blocking: true, Context: ctx}).
		Run(func(mock.Arguments) { cancel() }). // Side effect: cancel the Context.
		Return(ReadResult{Error: ErrNotBroker}, ioutil.NopCloser(nil))

	var recovered [13]byte

	// ReadFull drives required Gets. Expect the non-blocking ErrNotYetAvailable was surfaced.
	var n, err = io.ReadFull(rr, recovered[:])
	c.Check(n, gc.Equals, 3)
	c.Check(err, gc.Equals, ErrNotYetAvailable)
	c.Check(rr.Mark.Offset, gc.Equals, int64(103))

	rr.Blocking = true

	// Re-enter the read. Expect it succeeds.
	n, err = io.ReadFull(rr, recovered[n:])
	c.Check(n, gc.Equals, 10)
	c.Check(string(recovered[:]), gc.Equals, "foobarbazbing")
	c.Check(err, gc.IsNil)
	c.Check(rr.Mark.Offset, gc.Equals, int64(213))

	// Next read attempt will fail with a cancelled Context.
	n, err = io.ReadFull(rr, recovered[:])
	c.Check(n, gc.Equals, 3)
	c.Check(string(recovered[:3]), gc.Equals, "YYY")
	c.Check(err, gc.Equals, context.Canceled)
	c.Check(rr.Mark.Offset, gc.Equals, int64(216))

	// Expect all readers were closed.
	c.Check(rr.Close(), gc.IsNil)

	for _, r := range readers {
		<-r.closeCh
	}
}

func (s *IOSuite) TestSeeking(c *gc.C) {
	var readers = []struct {
		io.Reader
		closeCh
	}{
		{strings.NewReader("abcdefghijk"), make(closeCh)},
		{strings.NewReader("foobar"), make(closeCh)},
		{strings.NewReader("xyz"), make(closeCh)},
	}

	var getter = new(MockGetter)
	var ctx = context.Background()
	var rr = NewRetryReaderContext(ctx, Mark{"a/journal", 0}, getter)

	var checkRead = func(expect string) {
		var buffer = make([]byte, len(expect))

		var n, err = rr.Read(buffer[:])
		c.Check(n, gc.Equals, len(expect))
		c.Check(err, gc.IsNil)
		c.Check(string(buffer[:n]), gc.Equals, expect)
	}

	// Seek of a closed reader just updates the offset.
	var n, err = rr.Seek(100, io.SeekStart)
	c.Check(n, gc.Equals, int64(100))
	c.Check(err, gc.IsNil)

	// Initial read opens reader.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 100, Blocking: true, Context: ctx}).
		Return(ReadResult{
			Offset:   100,
			Fragment: Fragment{End: 111},
		}, readers[0]).Once()

	// Read first three bytes.
	checkRead("abc")

	// Seek forward 3 bytes. It's satisfied by the current reader.
	n, err = rr.Seek(3, io.SeekCurrent)
	c.Check(n, gc.Equals, int64(106))
	c.Check(err, gc.IsNil)
	c.Check(rr.ReadCloser, gc.NotNil)

	// Read next three bytes. Expect Seek was applied.
	checkRead("ghi")

	// Seek forward 2 bytes. Cannot be satisfied by the current reader.
	n, err = rr.Seek(2, io.SeekCurrent)
	c.Check(n, gc.Equals, int64(111))
	c.Check(err, gc.IsNil)
	c.Check(rr.ReadCloser, gc.IsNil)

	// Next Read issues a new request.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 111, Blocking: true, Context: ctx}).
		Return(ReadResult{
			Offset:   111,
			Fragment: Fragment{End: 117},
		}, readers[1]).Once()

	checkRead("foo")

	// Seek backward 1 byte. Backward seeks force a re-open.
	n, err = rr.Seek(-1, io.SeekCurrent)
	c.Check(n, gc.Equals, int64(113))
	c.Check(err, gc.IsNil)
	c.Check(rr.ReadCloser, gc.IsNil)

	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 113, Blocking: true, Context: ctx}).
		Return(ReadResult{Offset: 113, Fragment: Fragment{End: 116}}, readers[2]).Once()

	checkRead("xyz")

	// io.SeekEnd is not supported.
	n, err = rr.Seek(-1, io.SeekEnd)
	c.Check(n, gc.Equals, int64(116)) // Not modified.
	c.Check(err, gc.ErrorMatches, "io.SeekEnd whence is not supported")
}

type closeCh chan struct{}

func (c closeCh) Close() error {
	close(c)
	return nil
}

var _ = gc.Suite(&IOSuite{})
