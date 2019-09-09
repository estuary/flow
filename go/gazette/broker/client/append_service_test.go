package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
)

type AppendServiceSuite struct{}

func (s *AppendServiceSuite) TestBasicAppendWithRetry(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString("hello, world")
	c.Assert(aa.Release(), gc.IsNil)

	readHelloWorldAppendRequest(c, broker) // RPC is dispatched to broker.

	// Interlude: expect |aa.mu| remains lockable while |aa| is executed,
	// and that |aa| was chained with a new & empty AsyncAppend.
	aa.mu.Lock()
	c.Check(aa.checkpoint, gc.Equals, int64(12))
	c.Check(aa.next, gc.NotNil)
	c.Check(aa.next.checkpoint, gc.Equals, int64(0))
	c.Check(aa.next.fb, gc.IsNil) // |next| has not been returned by StartAppend.
	c.Check(aa.next.next, gc.IsNil)
	c.Check(aa.mu == aa.next.mu, gc.Equals, true) // |mu| instance is shared.

	// Expect |aa.next| is now indexed by AppendService, rather than |aa|.
	c.Check(as.PendingExcept(""), gc.DeepEquals, OpFutures{aa.next: {}})
	aa.mu.Unlock()

	// First & second attempts fail. Expect RPC is retried until success.
	broker.WriteLoopErrCh <- errors.New("first attempt fails")
	readHelloWorldAppendRequest(c, broker) // Expect RPC is retried.
	broker.WriteLoopErrCh <- errors.New("second attempt fails")
	readHelloWorldAppendRequest(c, broker)

	// serveAppends sets |aa.next| to |aa| upon it's completion.
	var aaNext = aa.next
	broker.AppendRespCh <- buildAppendResponseFixture(broker) // Success.

	c.Check(aa.Err(), gc.IsNil)
	aa.mu.Lock()
	c.Check(aa.Response(), gc.DeepEquals, buildAppendResponseFixture(broker))
	c.Check(aa.next == aa, gc.Equals, true)
	aa.mu.Unlock()

	// After completing |aa|, the service loop trivially resolved |aaNext|
	// as it was still in its initial state, and exited.
	c.Check(aaNext.Err(), gc.IsNil)
	aaNext.mu.Lock()
	c.Check(aaNext.next == aaNext, gc.Equals, true)
	aaNext.mu.Unlock()

	c.Check(as.PendingExcept(""), gc.HasLen, 0)
}

func (s *AppendServiceSuite) TestAppendPipelineWithAborts(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var serveCh, cleanup = gateServeAppends()
	defer cleanup()

	var aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)

	// Fix a buffer size larger than a single write, but smaller than the concatenation
	// to ensure abort rollbacks spill across both the backing file and buffer.
	aa.fb.buf = bufio.NewWriterSize(aa.fb, 7)

	_, _ = aa.Writer().WriteString("aborted first write")
	aa.Require(errors.New("whoops"))
	c.Check(aa.Release(), gc.ErrorMatches, "whoops")

	aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString("write one")
	c.Check(aa.Release(), gc.IsNil)

	aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString("ABT")
	aa.Require(errors.New("potato"))
	c.Check(aa.Release(), gc.ErrorMatches, "potato")

	aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString(" write two")
	c.Assert(aa.Release(), gc.IsNil)

	aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString("ABORT ABORT")
	aa.Require(errors.New("tomato"))
	c.Check(aa.Release(), gc.ErrorMatches, "tomato")

	// Start serveAppends, and expect one Append RPC which reflects writes & rollbacks.
	close(serveCh)
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Journal: "a/journal"})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Content: []byte("write one write two")})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{})
	c.Check(<-broker.ReadLoopErrCh, gc.Equals, io.EOF)

	broker.AppendRespCh <- buildAppendResponseFixture(broker)
	<-aa.Done()

	c.Check(aa.Response(), gc.DeepEquals, buildAppendResponseFixture(broker))

	for op := range as.PendingExcept("") {
		<-op.Done()
	}
}

func (s *AppendServiceSuite) TestAppendSizeCutoff(c *gc.C) {
	defer func(s int64) { appendBufferCutoff = s }(appendBufferCutoff)
	appendBufferCutoff = 8

	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var serveCh, cleanup = gateServeAppends()
	defer cleanup()

	var chs []<-chan struct{}

	for i := 0; i != 3; i++ {
		var aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
		_, _ = aa.Writer().WriteString("hello, ")
		_, _ = aa.Writer().WriteString("world")

		c.Check(aa.Release(), gc.IsNil)
		chs = append(chs, aa.Done())
	}

	// Expect each "hello, world" was grouped into a separate chained RPC.
	close(serveCh)
	for i := 0; i != 3; i++ {
		readHelloWorldAppendRequest(c, broker)
		broker.AppendRespCh <- buildAppendResponseFixture(broker)
		<-chs[i]
	}
	for op := range as.PendingExcept("") {
		<-op.Done()
	}
}

func (s *AppendServiceSuite) TestAppendRacesServiceLoop(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var aa1 = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa1.Writer().WriteString("hello, world")
	c.Check(aa1.Release(), gc.IsNil)

	readHelloWorldAppendRequest(c, broker)

	// When |aa1|'s RPC began, it was chained into |aa2|.
	// Install and lock a different mutex on |aa2|.
	var aa2 = as.appends["a/journal"]
	c.Check(aa1.next, gc.Equals, aa2)

	// When |aa1|'s RPC completes, serveAppends will re-lock |aa1.mu|, step to
	// |aa2|, and on realizing it's trivially completed, will mark it as completed
	// and then unlock |aa2.mu| (which ordinarily is the same Mutex as |aa1.mu|).
	//
	// We swap out the |aa2| Mutex here so that StartAppend blocks, with the
	// expectation that it will be unblocked when serveAppends unlocks it
	// as per the flow above.
	aa2.mu = new(sync.Mutex)
	aa2.mu.Lock()

	broker.AppendRespCh <- buildAppendResponseFixture(broker)

	// Begin an Append, which will grab |aa2| from the index and blocks on
	// our locked |aa2.mu| fixture until serveAppends unlocks. On obtaining
	// the lock, StartAppend will realize |aa2| is completed and try again.
	var aa3 = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)

	aa2.mu.Lock() // Make race-detector happy.
	c.Check(aa2.next == aa2, gc.Equals, true)
	c.Check(aa3 != aa2, gc.Equals, true)

	_, _ = aa3.Writer().WriteString("hello, world")
	c.Check(aa3.Release(), gc.IsNil)

	readHelloWorldAppendRequest(c, broker)
	broker.AppendRespCh <- buildAppendResponseFixture(broker)

	for op := range as.PendingExcept("") {
		<-op.Done()
	}
}

func (s *AppendServiceSuite) TestAppendContextCancellation(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())

	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(ctx, rjc)

	var aa1 = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa1.Writer().WriteString("hello, world")
	c.Check(aa1.Release(), gc.IsNil)

	// Read the request, but don't respond.
	readHelloWorldAppendRequest(c, broker)

	// Start a second, dependent append.
	var aa2 = as.StartAppend(pb.AppendRequest{Journal: "other/journal"}, OpFutures{aa1: {}})
	_, _ = aa2.Writer().WriteString("another write")
	c.Check(aa2.Release(), gc.IsNil)

	cancel()
	broker.WriteLoopErrCh <- nil // |broker| already read EOF; now its RPC returns.

	// Expect both AsyncAppends were aborted.
	<-aa1.Done()
	<-aa2.Done()

	c.Check(aa1.Err(), gc.Equals, context.Canceled)
	c.Check(aa2.Err(), gc.ErrorMatches, "dependency failed: context canceled")

	// New appends may be started without issue, but abort immediately.
	var aa3 = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa3.Writer().WriteString("final write")
	c.Check(aa3.Release(), gc.IsNil)

	<-aa3.Done()
	c.Check(aa3.Err(), gc.Equals, context.Canceled)
}

func (s *AppendServiceSuite) TestFlushErrorHandlingCases(c *gc.C) {
	var mf = mockFile{n: 6}

	var fb = &appendBuffer{file: &mf}
	fb.buf = bufio.NewWriterSize(fb, 8)

	// Case 1: flush succeeds.
	_, _ = fb.buf.Write([]byte("XXX"))
	c.Check(fb.flush(), gc.IsNil)
	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXX")
	c.Check(fb.buf.Buffered(), gc.Equals, 0)
	c.Check(fb.buf.Available(), gc.Equals, 8)
	c.Check(fb.offset, gc.Equals, int64(3))

	// Case 2: flush fails after partial write.
	_, _ = fb.buf.Write([]byte("YYYhello"))

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

	_, _ = fb.buf.Write([]byte("world"))
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

	_, _ = fb.buf.Write([]byte("!"))
	c.Check(fb.flush(), gc.IsNil)

	c.Check(mf.Buffer.String(), gc.DeepEquals, "XXXYYYhelloworld!")
	c.Check(fb.buf.Buffered(), gc.Equals, 0)
}

func (s *AppendServiceSuite) TestReleaseChecksForWriteErrorAndRecovers(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var serveCh, cleanup = gateServeAppends()
	defer cleanup()

	// Begin a write with a small buffer fixture which will fail.
	var aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	var mf = mockFile{n: 2}
	aa.fb = &appendBuffer{file: &mf}
	aa.fb.buf = bufio.NewWriterSize(aa.fb, 7)

	var _, err = aa.Writer().WriteString("write spills to |mf| and fails")
	c.Check(err, gc.Equals, io.ErrShortWrite)

	mf.n = 100 // Allow write to proceed after next rollback.
	c.Check(aa.Release(), gc.Equals, io.ErrShortWrite)

	// Try again. This time the write proceeds.
	aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString("hello, world")
	c.Check(aa.Release(), gc.IsNil)

	close(serveCh)
	readHelloWorldAppendRequest(c, broker) // RPC is dispatched to broker.
	broker.AppendRespCh <- buildAppendResponseFixture(broker)

	<-aa.Done()
	c.Check(aa.Response(), gc.DeepEquals, buildAppendResponseFixture(broker))

	for op := range as.PendingExcept("") {
		<-op.Done()
	}
}

func (s *AppendServiceSuite) TestAppendOrderingCycle(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var serveCh, cleanup = gateServeAppends()
	defer cleanup()

	// Begin a sequence of writes which is ultimately cyclic over journals.
	var expect = []struct {
		journal pb.Journal
		content string
	}{
		{"journal/A", "write one"},
		{"journal/B", "write two"},
		{"journal/C", "write three"},
		{"journal/B", "write four"},
		{"journal/A", "write five"},
		{"journal/C", "write six"},
	}

	for _, exp := range expect {
		var aa = as.StartAppend(pb.AppendRequest{Journal: exp.journal}, as.PendingExcept(exp.journal))
		_, _ = aa.Writer().WriteString(exp.content)
		c.Check(aa.Release(), gc.IsNil)

		// Start a second write which uses the same dependencies, and is batched.
		var aa2 = as.StartAppend(pb.AppendRequest{Journal: exp.journal}, as.PendingExcept(exp.journal))
		c.Check(aa2, gc.Equals, aa)

		_, _ = aa2.Writer().WriteString("!")
		c.Check(aa2.Release(), gc.IsNil)
	}

	close(serveCh) // Unblock raced service loops for each journal.

	// Expect that we properly read Append RPCs in dependency order.
	for i := range expect {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Journal: expect[i].journal})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Content: []byte(expect[i].content + "!")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{})
		c.Check(<-broker.ReadLoopErrCh, gc.Equals, io.EOF)

		broker.AppendRespCh <- buildAppendResponseFixture(broker)
	}
	for op := range as.PendingExcept("") {
		<-op.Done()
	}
}

func (s *AppendServiceSuite) TestDependencyError(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var as = NewAppendService(context.Background(), rjc)

	var dep1, dep2 = NewAsyncOperation(), NewAsyncOperation()

	// Start two appends with different dependencies, which chain to different *AsyncAppends.
	var aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, OpFutures{dep1: {}})
	_, _ = aa.Writer().WriteString("foo")
	c.Check(aa.Release(), gc.IsNil)

	var aa2 = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, OpFutures{dep2: {}})
	_, _ = aa2.Writer().WriteString("bar")
	c.Check(aa2.Release(), gc.IsNil)
	c.Check(aa == aa2, gc.Equals, false)

	// dep2 succeeds, but dep1 fails.
	dep2.Resolve(nil)
	dep1.Resolve(errors.New("an error"))

	// Expect both |aa| and |aa2| fail.
	<-aa.Done()
	c.Check(aa.Err(), gc.ErrorMatches, "dependency failed: an error")
	<-aa2.Done()
	c.Check(aa2.Err(), gc.ErrorMatches, "dependency failed: an error")

	// Start another append.
	aa = as.StartAppend(pb.AppendRequest{Journal: "a/journal"}, nil)
	_, _ = aa.Writer().WriteString("baz")
	c.Check(aa.Release(), gc.IsNil)

	// Confirm |aa| resulted in a new chain, that propagates the error.
	c.Check(aa != aa2.next, gc.Equals, true)

	<-aa.Done()
	c.Check(aa.Err(), gc.ErrorMatches, "dependency failed: an error")
}

func (s *AppendServiceSuite) TestBufferPooling(c *gc.C) {
	var ab = appendBufferPool.Get().(*appendBuffer)

	// Precondition: write some content.
	_, _ = ab.buf.WriteString("foobar")
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

func (s *AppendServiceSuite) TestSubsetCases(c *gc.C) {
	var mkAA = func(name pb.Journal) *AsyncAppend {
		return &AsyncAppend{app: *NewAppender(nil, nil, pb.AppendRequest{Journal: name})}
	}
	var set = func(l ...OpFuture) (out OpFutures) {
		out = make(OpFutures)
		for _, op := range l {
			out[op] = struct{}{}
		}
		return out
	}

	var A, B, C, D = mkAA("A"), mkAA("B"), mkAA("C"), mkAA("D")
	var C2 = *C

	// Case: Empty set is a subset of the empty set.
	c.Check(set().IsSubsetOf(set()), gc.Equals, true)
	// Case: Non-empty set is not a subset of the empty set.
	c.Check(set(D).IsSubsetOf(set()), gc.Equals, false)
	// Case: Any single item is a subset of all items.
	for aa := range set(A, B, C, D) {
		c.Check(set(aa).IsSubsetOf(set(A, B, C, D)), gc.Equals, true)
	}
	// Case: A set is a subset of its identity.
	c.Check(set(A, B, C, D).IsSubsetOf(set(A, B, C, D)), gc.Equals, true)
	// Case: However, a matched journal with a different *AsyncAppend differs.
	c.Check(set(A, B, C, D).IsSubsetOf(set(A, B, &C2, D)), gc.Equals, false)
	// Cases: Mixed subsets.
	c.Check(set(A, C).IsSubsetOf(set(A, B, C, D)), gc.Equals, true)
	c.Check(set(B, C).IsSubsetOf(set(A, B, C, D)), gc.Equals, true)
	c.Check(set(B, D).IsSubsetOf(set(A, B, C, D)), gc.Equals, true)
	// Cases: Mixed non-subsets.
	c.Check(set(A, B, C).IsSubsetOf(set(A, C, D)), gc.Equals, false)
	c.Check(set(B, C, D).IsSubsetOf(set(A, B, C)), gc.Equals, false)
	c.Check(set(A, C, D).IsSubsetOf(set(A, B, D)), gc.Equals, false)
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
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Journal: "a/journal"})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Content: []byte("hello, world")})
	c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{})
	c.Check(<-broker.ReadLoopErrCh, gc.Equals, io.EOF)
}

func gateServeAppends() (chan<- struct{}, func()) {
	var ch = make(chan struct{})
	var realServeAppends = serveAppends

	serveAppends = func(s *AppendService, aa *AsyncAppend, err error) {
		<-ch
		realServeAppends(s, aa, err)
	}
	return ch, func() { serveAppends = realServeAppends }
}

func buildAppendResponseFixture(ep interface{ Endpoint() pb.Endpoint }) pb.AppendResponse {
	return pb.AppendResponse{
		Status: pb.Status_OK,
		Header: *buildHeaderFixture(ep),
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            100,
			End:              106,
			Sum:              pb.SHA1SumOf("hello, world"),
			CompressionCodec: pb.CompressionCodec_NONE,
		},
		Registers: new(pb.LabelSet),
	}
}

type testOpFuture struct {
	done chan struct{}
	err  error
}

func (t testOpFuture) Done() <-chan struct{} { return t.done }
func (t testOpFuture) Err() error            { return t.err }

var _ = gc.Suite(&AppendServiceSuite{})
