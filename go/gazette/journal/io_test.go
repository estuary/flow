package journal

import (
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing/iotest"
	"time"

	gc "github.com/go-check/check"
)

type IOSuite struct{}

func (s *IOSuite) TestMarkedReaderUpdates(c *gc.C) {
	reader := struct {
		io.Reader
		closeCh
	}{iotest.TimeoutReader(iotest.HalfReader(strings.NewReader("afixture"))), make(closeCh)}

	mr := NewMarkedReader(Mark{Journal: "a/journal", Offset: 1234}, reader)
	var buffer [8]byte

	// First read: read succeeds at half of the request size.
	n, err := mr.Read(buffer[:6])
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

func (s *IOSuite) TestReaderRetries(c *gc.C) {
	oldCooloff := retryReaderErrCooloff
	defer func() { retryReaderErrCooloff = oldCooloff }()
	retryReaderErrCooloff = time.Nanosecond

	// Sequence of test readers which will be returned by sequential Get's.
	readers := []struct {
		io.Reader
		closeCh
	}{
		{iotest.OneByteReader(strings.NewReader("foo")), make(closeCh)},
		{iotest.TimeoutReader(iotest.HalfReader(strings.NewReader("barbaXXX"))), make(closeCh)},
		{strings.NewReader(""), make(closeCh)},
		{iotest.HalfReader(strings.NewReader("zbingYYY")), make(closeCh)},
	}

	// Initial read of 3 bytes, which increments the offset from 0 and then EOFs.
	getter := &MockGetter{}
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: -1, Blocking: true}).
		Return(ReadResult{Offset: 100}, readers[0]).Once()

	// Next open fails.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Blocking: true}).
		Return(ReadResult{Error: ErrNotBroker}, ioutil.NopCloser(nil)).Once()

	// Next open jumps the offset, reads 5 bytes, then times out.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Blocking: true}).
		Return(ReadResult{Offset: 203}, readers[1]).Once()

	// Next open returns a reader which immediately EOFs.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 208, Blocking: true}).
		Return(ReadResult{Offset: 208}, readers[2]).Once()

	// Next open reads remaining 5 bytes at the updated offset.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 208, Blocking: true}).
		Return(ReadResult{Offset: 208}, readers[3]).Once()

	var recovered [13]byte

	rr := NewRetryReader(Mark{"a/journal", -1}, getter)

	// Expect that ReadFull() drives required Get()s and completes without error.
	n, err := io.ReadFull(rr, recovered[:])
	c.Check(rr.Mark.Offset, gc.Equals, int64(213))

	c.Check(n, gc.Equals, 13)
	c.Check(string(recovered[:]), gc.Equals, "foobarbazbing")
	c.Check(err, gc.IsNil)

	// Expect all readers were closed.
	c.Check(rr.Close(), gc.IsNil)

	for _, r := range readers {
		<-r.closeCh
	}
}

func (s *IOSuite) TestEOFTimeout(c *gc.C) {
	defer func() {
		timeNow = time.Now
	}()
	timeNow = func() time.Time { return time.Unix(1234, 0) }

	// Sequence of test readers which will be returned by sequential Get's.
	readers := []struct {
		io.Reader
		closeCh
	}{
		{strings.NewReader("foo"), make(closeCh)},
		{strings.NewReader(""), make(closeCh)},
	}

	getter := &MockGetter{}
	rr := NewRetryReader(Mark{"a/journal", 0}, getter)
	rr.EOFTimeout = time.Second

	// Initial read opens reader.
	deadline := time.Unix(1234, 0).Add(time.Second)
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 0, Deadline: deadline}).
		Return(ReadResult{Offset: 100}, readers[0]).Once()

	var buffer [12]byte

	n, err := rr.Read(buffer[:])
	c.Check(n, gc.Equals, 3)
	c.Check(string(buffer[:n]), gc.Equals, "foo")
	c.Check(err, gc.IsNil)

	// Next Read gets EOF, and silently closes.
	n, err = rr.Read(buffer[:])
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.IsNil)

	// Final Read opens a new empty reader. Error is passed through.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 103, Deadline: deadline}).
		Return(ReadResult{Offset: 103}, readers[1]).Once()

	n, err = rr.Read(buffer[:])
	c.Check(n, gc.Equals, 0)
	c.Check(err, gc.Equals, io.EOF)
}

func (s *IOSuite) TestSeeking(c *gc.C) {
	readers := []struct {
		io.Reader
		closeCh
	}{
		{strings.NewReader("abcdefghijk"), make(closeCh)},
		{strings.NewReader("foobar"), make(closeCh)},
		{strings.NewReader("xyz"), make(closeCh)},
	}

	getter := &MockGetter{}
	rr := NewRetryReader(Mark{"a/journal", 0}, getter)

	checkRead := func(expect string) {
		var buffer = make([]byte, len(expect))

		n, err := rr.Read(buffer[:])
		c.Check(n, gc.Equals, len(expect))
		c.Check(err, gc.IsNil)
		c.Check(string(buffer[:n]), gc.Equals, expect)
	}

	// Seek of a closed reader just updates the offset.
	n, err := rr.Seek(100, os.SEEK_SET)
	c.Check(n, gc.Equals, int64(100))
	c.Check(err, gc.IsNil)

	// Initial read opens reader.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 100, Blocking: true}).
		Return(ReadResult{
			Offset:   100,
			Fragment: Fragment{End: 111},
		}, readers[0]).Once()

	// Read first three bytes.
	checkRead("abc")

	// Seek forward 3 bytes. It's satisfied by the current reader.
	n, err = rr.Seek(3, os.SEEK_CUR)
	c.Check(n, gc.Equals, int64(106))
	c.Check(err, gc.IsNil)
	c.Check(rr.ReadCloser, gc.NotNil)

	// Read next three bytes. Expect Seek was applied.
	checkRead("ghi")

	// Seek forward 2 bytes. Cannot be satisfied by the current reader.
	n, err = rr.Seek(2, os.SEEK_CUR)
	c.Check(n, gc.Equals, int64(111))
	c.Check(err, gc.IsNil)
	c.Check(rr.ReadCloser, gc.IsNil)

	// Next Read issues a new request.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 111, Blocking: true}).
		Return(ReadResult{
			Offset:   111,
			Fragment: Fragment{End: 117},
		}, readers[1]).Once()

	checkRead("foo")

	// Seek backward 1 byte. Backward seeks force a re-open.
	n, err = rr.Seek(-1, os.SEEK_CUR)
	c.Check(n, gc.Equals, int64(113))
	c.Check(err, gc.IsNil)
	c.Check(rr.ReadCloser, gc.IsNil)

	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 113, Blocking: true}).
		Return(ReadResult{Offset: 113, Fragment: Fragment{End: 116}}, readers[2]).Once()

	checkRead("xyz")

	// SEEK_END is not supported.
	n, err = rr.Seek(-1, os.SEEK_END)
	c.Check(n, gc.Equals, int64(116)) // Not modified.
	c.Check(err, gc.ErrorMatches, "invalid whence")
}

type closeCh chan struct{}

func (c closeCh) Close() error {
	close(c)
	return nil
}

var _ = gc.Suite(&IOSuite{})
