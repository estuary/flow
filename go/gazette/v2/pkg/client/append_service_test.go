package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	gc "github.com/go-check/check"

	"github.com/LiveRamp/gazette/pkg/broker/teststub"
	pb "github.com/LiveRamp/gazette/pkg/protocol"
)

type AppendServiceSuite struct{}

func (s *AppendServiceSuite) TestBasicAppendWithRetry(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var as = NewAppendService(ctx, broker.MustClient())

	var aa = as.StartAppend("a/journal")
	aa.Writer().WriteString("hello, world")
	c.Assert(aa.Release(), gc.IsNil)

	readHelloWorldAppendRequest(c, broker) // RPC is dispatched to broker.

	// Interlude: expect |aa.mu| remains lockable while |aa| is executed,
	// and that |aa| was chained with a new & empty AsyncAppend.
	aa.mu.Lock()
	c.Check(aa.checkpoint, gc.Equals, int64(12))
	c.Check(aa.next, gc.NotNil)
	c.Check(aa.next.checkpoint, gc.Equals, int64(0))
	c.Check(aa.next.next, gc.IsNil)
	aa.mu.Unlock()

	broker.ErrCh <- errors.New("first attempt fails")

	readHelloWorldAppendRequest(c, broker) // RPC is retried.
	broker.AppendRespCh <- appendResponseFixture

	<-aa.Done()
	c.Check(aa.Response(), gc.DeepEquals, *appendResponseFixture)

	as.Wait() // Expect journal service loop exits.
	c.Check(aa.next.next, gc.Equals, tombstoneAsyncAppend)
}

func (s *AppendServiceSuite) TestAppendPipelineWithAborts(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var as = NewAppendService(ctx, broker.MustClient())

	// Empty append is begun, and dispatched to broker.
	var aa1 = as.StartAppend("a/journal")
	c.Assert(aa1.Release(), gc.IsNil)

	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
	c.Check(<-broker.AppendReqCh, gc.IsNil)

	// While that's processing, a new set of writes, with interleaved aborts, collect.
	var aa2 = as.StartAppend("a/journal")

	// Fix a buffer size larger than a single write, but smaller than the concatenation
	// to ensure abort rollbacks spill across both the backing file and buffer.
	aa2.fb.buf = bufio.NewWriterSize(aa2.fb, 7)

	aa2.Writer().WriteString("write one")
	c.Check(aa2.Release(), gc.IsNil)

	aa2 = as.StartAppend("a/journal")
	aa2.Writer().WriteString("ABT")
	aa2.Require(errors.New("potato"))
	c.Check(aa2.Release(), gc.ErrorMatches, "potato")

	aa2 = as.StartAppend("a/journal")
	aa2.Writer().WriteString(" write two")
	c.Assert(aa2.Release(), gc.IsNil)

	aa2 = as.StartAppend("a/journal")
	aa2.Writer().WriteString("ABORT ABORT")
	aa2.Require(errors.New("tomato"))
	c.Check(aa2.Release(), gc.ErrorMatches, "tomato")

	// First response completes, and second request begins.
	broker.AppendRespCh <- appendResponseFixture
	<-aa1.Done()

	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("write one write two")})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
	c.Check(<-broker.AppendReqCh, gc.IsNil)

	broker.AppendRespCh <- appendResponseFixture
	<-aa2.Done()

	c.Check(aa2.Response(), gc.DeepEquals, *appendResponseFixture)

	as.Wait() // Expect journal service loop exits.
}

func (s *AppendServiceSuite) TestAppendSizeCuttoff(c *gc.C) {
	defer func(s int64) { appendBufferCutoff = s }(appendBufferCutoff)
	appendBufferCutoff = 8

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var as = NewAppendService(ctx, broker.MustClient())

	var chs []<-chan struct{}

	for i := 0; i != 3; i++ {
		var aa = as.StartAppend("a/journal")
		aa.Writer().WriteString("hello, ")
		c.Check(aa.Release(), gc.IsNil)

		aa = as.StartAppend("a/journal")
		aa.Writer().WriteString("world")

		c.Assert(aa.Release(), gc.IsNil)
		chs = append(chs, aa.Done())
	}

	// Expect each "hello, world" was grouped into a separate chained RPC.
	for i := 0; i != 3; i++ {
		readHelloWorldAppendRequest(c, broker)
		broker.AppendRespCh <- appendResponseFixture
		<-chs[i]
	}
	as.Wait()
}

func (s *AppendServiceSuite) TestAppendRacesServiceLoop(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var as = NewAppendService(ctx, broker.MustClient())

	var aa1 = as.StartAppend("a/journal")
	aa1.Writer().WriteString("hello, world")
	c.Check(aa1.Release(), gc.IsNil)

	readHelloWorldAppendRequest(c, broker)

	// When |aa1|'s RPC began, it was chained into |aa2|.
	// Install and lock a different mutex on |aa2|.
	var aa2 = as.appends["a/journal"]
	c.Check(aa1.next, gc.Equals, aa2)
	as.appends["a/journal"].mu = new(sync.Mutex)
	as.appends["a/journal"].mu.Lock()

	broker.AppendRespCh <- appendResponseFixture

	// Begin an Append, which will grab |aa2| from the index and block on |aa2.mu|.
	// The service loop will unlock |aa2.mu| just before exit. On obtaining the
	// lock, StartAppend will realize |aa2| is a tombstone and try again.
	var aa3 = as.StartAppend("a/journal")

	c.Check(aa2.next, gc.Equals, tombstoneAsyncAppend)
	c.Check(aa3, gc.Not(gc.Equals), aa2)

	aa3.Require(errors.New("abort")).Release()
	as.Wait()
}

func (s *AppendServiceSuite) TestFlushErrorHandlingCases(c *gc.C) {
	var mf = mockFile{n: 6}

	var fb = &appendBuffer{file: &mf}
	fb.buf = bufio.NewWriterSize(fb, 8)

	// Case 1: flush succeeds.
	fb.buf.Write([]byte("XXX"))
	c.Check(fb.flush(), gc.IsNil)
	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXX")
	c.Check(fb.buf.Buffered(), gc.Equals, 0)
	c.Check(fb.buf.Available(), gc.Equals, 8)
	c.Check(fb.offset, gc.Equals, int64(3))

	// Case 2: flush fails after partial write.
	fb.buf.Write([]byte("YYYhello"))

	// Precondition: buffer is fully filled.
	c.Check(fb.buf.Buffered(), gc.Equals, 8)
	c.Check(fb.buf.Available(), gc.Equals, 0)

	// Expect we flush until the writer errors. Repeated flush attempts are non-destructive.
	for i := 0; i != 3; i++ {
		c.Check(fb.flush(), gc.Equals, io.ErrShortWrite)
		c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYY")
		c.Check(mf.n, gc.Equals, 0)
		c.Check(fb.buf.Buffered(), gc.Equals, 5)
		c.Check(fb.buf.Available(), gc.Equals, 3)
		c.Check(fb.offset, gc.Equals, int64(6))
	}

	mf.n = 5
	c.Check(fb.flush(), gc.IsNil)
	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhello")
	c.Check(mf.n, gc.Equals, 0)
	c.Check(fb.buf.Buffered(), gc.Equals, 0)
	c.Check(fb.buf.Available(), gc.Equals, 8)
	c.Check(fb.offset, gc.Equals, int64(11))

	// Case 3: buffer is precisely full, and flush fails with no progress.
	fb.buf = bufio.NewWriterSize(fb, 5)

	fb.buf.Write([]byte("world"))
	c.Check(fb.buf.Available(), gc.Equals, 0)

	for i := 0; i != 3; i++ {
		c.Check(fb.flush(), gc.Equals, io.ErrShortWrite)
		c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhello")
		c.Check(mf.n, gc.Equals, 0)
		c.Check(fb.buf.Buffered(), gc.Equals, 5)
		c.Check(fb.buf.Available(), gc.Equals, 0)
		c.Check(fb.offset, gc.Equals, int64(11))
	}

	mf.n = 5
	c.Check(fb.flush(), gc.IsNil)
	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhelloworld")
	c.Check(mf.n, gc.Equals, 0)
	c.Check(fb.buf.Buffered(), gc.Equals, 0)
	c.Check(fb.buf.Available(), gc.Equals, 5)
	c.Check(fb.offset, gc.Equals, int64(16))

	// Case 4: buffer write triggers flush, which fails.
	mf.n = 4

	var n, err = fb.buf.Write([]byte("123"))
	c.Check(n, gc.Equals, 3)
	c.Check(err, gc.IsNil)

	n, err = fb.buf.Write([]byte("456"))
	c.Check(n, gc.Equals, 2)
	c.Check(err, gc.Equals, io.ErrShortWrite)

	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhelloworld1234")
	c.Check(fb.buf.Buffered(), gc.Equals, 1)
	c.Check(fb.flush(), gc.Equals, io.ErrShortWrite)

	// Model a roll-back, by flushing and then seeking to a prior checkpoint.

	mf.n = 100
	c.Check(fb.flush(), gc.IsNil)
	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhelloworld12345")
	c.Check(fb.buf.Buffered(), gc.Equals, 0)

	// '6' was lost, as it didn't fit in the buffer and
	// was discarded with the encountered flush error.

	c.Check(fb.offset, gc.Equals, int64(21))
	fb.offset = 16
	c.Check(fb.seek(), gc.IsNil)

	fb.buf.Write([]byte("!"))
	c.Check(fb.flush(), gc.IsNil)

	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhelloworld!")
	c.Check(fb.buf.Buffered(), gc.Equals, 0)
}

func (s *AppendServiceSuite) TestBeginCommitErrorAndRecover(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = teststub.NewBroker(c, ctx)
	var as = NewAppendService(ctx, broker.MustClient())

	var aa = as.StartAppend("a/journal")

	// Swap in a small buffer fixture which will fail to write.
	var mf = mockFile{n: 2}
	aa.fb = &appendBuffer{file: &mf}
	aa.fb.buf = bufio.NewWriterSize(aa.fb, 7)

	aa.Writer().WriteString("hello, world")
	c.Check(aa.Release(), gc.Equals, io.ErrShortWrite)

	// Try again, and allow write to proceed.
	mf.n = 100
	aa = as.StartAppend("a/journal")
	aa.Writer().WriteString("hello, world")
	c.Check(aa.Release(), gc.IsNil)

	readHelloWorldAppendRequest(c, broker) // RPC is dispatched to broker.
	broker.AppendRespCh <- appendResponseFixture

	<-aa.Done()
	c.Check(aa.Response(), gc.DeepEquals, *appendResponseFixture)

	as.Wait() // Expect journal service loop exits.
}

func (s *AppendServiceSuite) TestBufferPooling(c *gc.C) {
	var ab = appendBufferPool.Get().(*appendBuffer)

	// Precondition: write some content.
	ab.buf.WriteString("foobar")
	c.Check(ab.buf.Flush(), gc.IsNil)
	c.Check(ab.offset, gc.Equals, int64(6))

	releaseFileBuffer(ab)
	ab = appendBufferPool.Get().(*appendBuffer)

	// Post-condition: a released and re-fetched buffer is zeroed.
	var n, err = ab.file.Seek(0, io.SeekCurrent)
	c.Check(ab.offset, gc.Equals, int64(0))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, int64(0))
}

// mockFile delegates to a Buffer, but limits total writes to |n| bytes of
// content. mockFile is technically an invalid Writer, since it returns
// err == nil with n < len(p), but bufio.Writer explicitly handles this case
// and we exercise it here.
type mockFile struct {
	n int
	bytes.Buffer
}

func (mf *mockFile) Write(p []byte) (n int, err error) {
	if n = len(p); n > mf.n {
		n = mf.n
	}
	mf.n -= n
	return mf.Buffer.Write(p[:n])
}

func (mf *mockFile) ReadAt(p []byte, offset int64) (int, error) {
	return bytes.NewReader(mf.Buffer.Bytes()).ReadAt(p, offset)
}

func (mf *mockFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// Pass.
	case io.SeekCurrent, io.SeekEnd:
		offset += int64(mf.Buffer.Len())
	}
	if int(offset) > mf.Buffer.Len() {
		panic("invalid offset")
	}
	mf.Buffer.Truncate(int(offset))
	return offset, nil
}

func readHelloWorldAppendRequest(c *gc.C, broker *teststub.Broker) {
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Journal: "a/journal"})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{Content: []byte("hello, world")})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, &pb.AppendRequest{})
	c.Check(<-broker.AppendReqCh, gc.IsNil) // Client EOF.
}

var (
	_ = gc.Suite(&AppendServiceSuite{})

	appendResponseFixture = &pb.AppendResponse{
		Status: pb.Status_OK,
		Header: headerFixture,
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            100,
			End:              106,
			Sum:              pb.SHA1SumOf("hello, world"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
	}
)
