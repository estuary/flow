package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"testing/iotest"

	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type RetrySuite struct{}

func (s *RetrySuite) TestReaderRetries(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), pb.NoopDispatchRouter{})

	var rr = NewRetryReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 100})
	c.Check(rr.Offset(), gc.Equals, int64(100))
	c.Check(rr.Journal(), gc.Equals, pb.Journal("a/journal"))

	go serveReadFixtures(c, broker,
		readFixture{content: "foo", err: errors.New("whoops")},
		readFixture{content: "barba", err: errors.New("whoops")},
		readFixture{status: pb.Status_NOT_JOURNAL_BROKER},
		readFixture{content: "zbing", status: pb.Status_OFFSET_NOT_YET_AVAILABLE},

		readFixture{content: "next "},
		readFixture{status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
		readFixture{content: "read"},

		readFixture{offset: 512, content: "xxxxyyyy"},
	)

	// Expect reads are retried through OFFSET_NOT_YET_AVAILABLE, which is surfaced to the caller.
	var b, err = ioutil.ReadAll(rr)
	c.Check(string(b), gc.Equals, "foobarbazbing")
	c.Check(err, gc.Equals, ErrOffsetNotYetAvailable)
	c.Check(rr.Offset(), gc.Equals, int64(100+13))

	// Toggle to blocking mode.
	rr.Reader.Request.Block = true

	// We're able to continue reading. The next OFFSET_NOT_YET_AVAILABLE is swallowed,
	// and reads continue until an offset jump is detected, which is surfaced.
	b, err = ioutil.ReadAll(rr)
	c.Check(string(b), gc.Equals, "next read")
	c.Check(err, gc.Equals, ErrOffsetJump)
	c.Check(rr.Offset(), gc.Equals, int64(512))

	go rr.Cancel()

	b, err = ioutil.ReadAll(rr)
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *RetrySuite) TestMisbehavingReaderCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), pb.NoopDispatchRouter{})

	// Construct a variety of unusual underlying reader behaviors. Expect
	// RetryReader retries appropriately and recovers correct content in all cases.
	var cases = []io.Reader{
		// Returns content, then EOF.
		strings.NewReader("foobar"),
		// Returns content & EOF.
		iotest.DataErrReader(strings.NewReader("foobar")),
		// Returns content, then iotest.ErrTimeout.
		iotest.TimeoutReader(strings.NewReader("foobar")),
		// Returns content & iotest.ErrTimeout.
		iotest.DataErrReader(iotest.TimeoutReader(strings.NewReader("foobar"))),
		// Returns single bytes, then separate EOF.
		iotest.OneByteReader(strings.NewReader("foobar")),
	}
	for _, tc := range cases {
		var rr = NewRetryReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 100})
		rr.Reader.direct = ioutil.NopCloser(tc)

		go serveReadFixtures(c, broker,
			readFixture{content: "bazbing", status: pb.Status_OFFSET_NOT_YET_AVAILABLE},
		)
		// Expect reads are retried through OFFSET_NOT_YET_AVAILABLE, which is surfaced to the caller.
		var b, err = ioutil.ReadAll(rr)
		c.Check(string(b), gc.Equals, "foobarbazbing")
		c.Check(err, gc.Equals, ErrOffsetNotYetAvailable)
		c.Check(rr.Offset(), gc.Equals, int64(100+13))
	}
}

func (s *RetrySuite) TestSeeking(c *gc.C) {
	var frag, url, dir, cleanup = buildFragmentFixture(c)
	defer cleanup()
	defer InstallFileTransport(dir)()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), pb.NoopDispatchRouter{})

	// Start two read fixtures which both return fragment metadata & URL,
	// then EOF, causing Reader to directly open the fragment.
	go serveReadFixtures(c, broker,
		readFixture{fragment: &frag, fragmentUrl: url, offset: frag.Begin},
		readFixture{fragment: &frag, fragmentUrl: url},
	)

	var rr = NewRetryReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal"})

	// Read initial response message.
	var _, err = rr.Read(nil)
	c.Check(err, gc.Equals, ErrOffsetJump)
	c.Check(rr.Offset(), gc.Equals, frag.Begin)

	_, err = rr.Read(nil) // Opens fragment URL.
	c.Check(err, gc.IsNil)

	// Case: seeking forward works, so long as the Fragment covers the seek'd offset.
	offset, err := rr.Seek(5, io.SeekCurrent)
	c.Check(offset, gc.Equals, frag.Begin+5)
	c.Check(err, gc.IsNil)

	offset, err = rr.Seek(frag.Begin+6, io.SeekStart)
	c.Check(offset, gc.Equals, frag.Begin+6)
	c.Check(err, gc.IsNil)

	var b = make([]byte, 5)
	n, err := rr.Read(b[:])
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "ello,")
	c.Check(rr.Offset(), gc.Equals, frag.Begin+6+5)

	// Case: seeking backwards causes the reader to be canceled and restarted.
	offset, err = rr.Seek(-6, io.SeekCurrent)
	c.Check(err, gc.IsNil)

	_, err = rr.Read(b[:]) // Reads initial response message.
	c.Check(err, gc.IsNil)

	_, err = rr.Read(b[:]) // Opens fragment URL.
	c.Check(err, gc.IsNil)
	c.Check(string(b[:n]), gc.Equals, "hello")
}

func (s *RetrySuite) TestBufferedSeekAdjustment(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var rjc = pb.NewRoutedJournalClient(broker.MustClient(), pb.NoopDispatchRouter{})

	go serveReadFixtures(c, broker,
		readFixture{content: "foo\nbar\nbaz\n", offset: 100},
	)
	var rr = NewRetryReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal", Offset: 100})
	var br = bufio.NewReader(rr)

	// Peek consumes the entire read fixture.
	var b, err = br.Peek(12)
	c.Check(err, gc.IsNil)
	c.Check(string(b), gc.Equals, "foo\nbar\nbaz\n")

	str, err := br.ReadString('\n')
	c.Check(err, gc.IsNil)
	c.Check(str, gc.Equals, "foo\n")

	c.Check(rr.AdjustedOffset(br), gc.Equals, int64(104))
	c.Check(br.Buffered(), gc.Equals, 8)

	// Expect seek is performed by discarding from |br|.
	offset, err := rr.AdjustedSeek(2, io.SeekCurrent, br)
	c.Check(err, gc.IsNil)
	c.Check(offset, gc.Equals, int64(106))
	c.Check(br.Buffered(), gc.Equals, 6)

	str, err = br.ReadString('\n')
	c.Check(str, gc.Equals, "r\n")
	c.Check(err, gc.IsNil)

	// Again. This time, expect the reader is restarted to perform the seek.
	var readerCtx = rr.Reader.ctx

	offset, err = rr.AdjustedSeek(-3, io.SeekCurrent, br)
	c.Check(err, gc.IsNil)
	c.Check(offset, gc.Equals, int64(105))

	c.Check(br.Buffered(), gc.Equals, 0)        // Expect |br| was reset.
	c.Check(rr.Offset(), gc.Equals, int64(105)) // Reader restarted at the new offset.
	<-readerCtx.Done()                          // Previous reader context was canceled.
}

var _ = gc.Suite(&RetrySuite{})
