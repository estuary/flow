package journal

import (
	"io"
	"io/ioutil"
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
	oldCooloff := kRetryReaderErrCooloff
	defer func() { kRetryReaderErrCooloff = oldCooloff }()
	kRetryReaderErrCooloff = time.Nanosecond

	readers := []struct {
		io.Reader
		closeCh
	}{
		{iotest.OneByteReader(strings.NewReader("foo")), make(closeCh)},
		{iotest.TimeoutReader(iotest.HalfReader(strings.NewReader("barbaXXX"))), make(closeCh)},
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

	// Next open reads remaining 5 bytes at the updated offset.
	getter.On("Get", ReadArgs{Journal: "a/journal", Offset: 208, Blocking: true}).
		Return(ReadResult{Offset: 208}, readers[2]).Once()

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

type closeCh chan struct{}

func (c closeCh) Close() error {
	close(c)
	return nil
}

var _ = gc.Suite(&IOSuite{})
