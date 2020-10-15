package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// AppendService batches, dispatches, and (if needed) retries asynchronous
// Append RPCs. Use of an AppendService is appropriate for clients who make
// large numbers of small writes to a Journal, and where those writes may be
// pipelined and batched to amortize the cost of broker Append RPCs. It may
// also simplify implementations for clients who would prefer to simply have
// writes block until successfully committed, as opposed to handling errors
// and retries themselves.
//
// For each journal, AppendService manages an ordered list of AsyncAppends,
// each having buffered content to be appended. The list is dispatched in
// FIFO order by a journal-specific goroutine.
//
// AsyncAppends are backed by temporary files on the local disk rather
// than memory buffers. This minimizes the impact of buffering on the heap
// and garbage collector, and also makes AppendService tolerant to sustained
// service disruptions (up to the capacity of the disk).
//
// AppendService implements the AsyncJournalClient interface.
type AppendService struct {
	pb.RoutedJournalClient
	ctx     context.Context             // Context for all appends of this service.
	appends map[pb.Journal]*AsyncAppend // Index of the most-recent AsyncAppend.
	errs    map[pb.Journal]error        // Index of terminal errors.
	mu      sync.Mutex                  // Guards |appends| and |errs|.
}

// NewAppendService returns an AppendService with the provided Context and BrokerClient.
func NewAppendService(ctx context.Context, client pb.RoutedJournalClient) *AppendService {
	return &AppendService{
		ctx:                 ctx,
		RoutedJournalClient: client,
		appends:             make(map[pb.Journal]*AsyncAppend),
		errs:                make(map[pb.Journal]error),
	}
}

// OpFuture represents an operation which is executing in the background. The
// operation has completed when Done selects. Err may be invoked to determine
// whether the operation succeeded or failed.
type OpFuture interface {
	// Done selects when operation background execution has finished.
	Done() <-chan struct{}
	// Err blocks until Done() and returns the final error of the OpFuture.
	Err() error
}

// OpFutures is a set of OpFuture instances.
type OpFutures map[OpFuture]struct{}

// IsSubsetOf is true of this OpFutures is a subset of the other.
func (s OpFutures) IsSubsetOf(other OpFutures) bool {
	for f := range s {
		if _, ok := other[f]; !ok {
			return false
		}
	}
	return true
}

// AsyncOperation is a simple, minimal implementation of the OpFuture interface.
type AsyncOperation struct {
	doneCh chan struct{} // Closed to signal operation has completed.
	err    error         // Error on operation completion.
}

// NewAsyncOperation returns a new AsyncOperation.
func NewAsyncOperation() *AsyncOperation { return &AsyncOperation{doneCh: make(chan struct{})} }

// Done selects when Resolve is called.
func (o *AsyncOperation) Done() <-chan struct{} { return o.doneCh }

// Err blocks until Resolve is called, then returns its error.
func (o *AsyncOperation) Err() error {
	<-o.Done()
	return o.err
}

// Resolve marks the AsyncOperation as completed with the given error.
func (o *AsyncOperation) Resolve(err error) {
	o.err = err
	close(o.doneCh)
}

// FinishedOperation is a convenience that returns an already-resolved AsyncOperation.
func FinishedOperation(err error) OpFuture {
	var op = NewAsyncOperation()
	op.Resolve(err)
	return op
}

// AsyncJournalClient composes a RoutedJournalClient with an API for performing
// asynchronous Append operations.
type AsyncJournalClient interface {
	pb.RoutedJournalClient

	// StartAppend begins a new asynchronous Append RPC. The caller holds exclusive access
	// to the returned AsyncAppend, and must then:
	//  * Write content to its Writer.
	//  * Optionally Require that one or more errors are nil.
	//  * Release the AsyncAppend, allowing queued writes to commit or,
	//    if an error occurred, to roll back.
	//
	// For performance reasons, an Append will often be batched with other Appends
	// having identical AppendRequests which are dispatched to this AppendService,
	// and note the Response.Fragment will reflect the entire batch written to the
	// broker. In all cases, relative order of Appends is preserved. One or more
	// dependencies may optionally be supplied. The Append RPC will not begin
	// until all dependencies have completed without error. A failure of a
	// dependency will also permanently fail the returned AsyncAppend and prevent
	// any further appends to this journal from starting. For this reason, an
	// OpFuture should only fail if it also invalidates the AsyncJournalClient
	// (eg, because the client is scoped to a context which is invalidated by the
	// OpFuture failure).
	StartAppend(req pb.AppendRequest, dependencies OpFutures) *AsyncAppend

	// PendingExcept returns an OpFutures set of *AsyncAppend instances being
	// evaluated for all Journals other than |except|. It can be used to build
	// "barriers" which ensure that all pending appends have committed prior to the
	// commencement of an append which is about to be issued. Eg, given:
	//
	//   var op = as.StartAppend(pb.AppendRequest{Journal: "target"}, as.PendingExcept("target"))
	//   op.Writer().WriteString("checkpoint")
	//   op.Release()
	//
	// All ongoing appends to journals other than "target" are guaranteed to commit
	// before an Append RPC is begun which writes "checkpoint" to journal "target".
	// PendingExcept("") returns all pending AsyncAppends.
	//
	// If a prior journal append failed (eg, because its dependency failed) a
	// resolved OpFuture with that error will be included in returned OpFutures.
	// This ensures the error will properly cascade to an operation which may
	// depend on these OpFutures.
	PendingExcept(except pb.Journal) OpFutures
}

// StartAppend implements the AsyncJournalClient interface.
func (s *AppendService) StartAppend(req pb.AppendRequest, dependencies OpFutures) *AsyncAppend {
	// Fetch the current AsyncAppend for |name|, or start one if none exists.
	s.mu.Lock()
	var aa, ok = s.appends[req.Journal]
	var err = s.errs[req.Journal]

	if !ok {
		aa = &AsyncAppend{
			op:           *NewAsyncOperation(),
			app:          *NewAppender(s.ctx, s.RoutedJournalClient, req),
			dependencies: dependencies,
			mu:           new(sync.Mutex),
		}
		s.appends[req.Journal] = aa
	}
	s.mu.Unlock()

	// Acquire exclusive write access for journal |name|. This may race with
	// other writes in progress, and on mutex acquisition a different AsyncAppend
	// may now be current for the journal.
	aa.mu.Lock()

	// Start the service loop (if needed) *after* we acquire |aa.mu|, and
	// *before* we skip forward to the current AsyncAppend. This ensures
	// serveAppends starts from the first AsyncAppend of the chain, and
	// that it blocks until the client completes the first write.
	if !ok {
		go serveAppends(s, aa, err)
	}

	// Follow the chain to the current AsyncAppend.
	for aa.next != nil {
		aa = aa.next

		// It's possible that (while we were waiting for |aa.mu.Lock|)
		// serveAppends completed this AsyncAppend, which it marked by setting
		// |aa.next| to itself. Recurse to try again.
		if aa == aa.next {
			aa.mu.Unlock()
			return s.StartAppend(req, dependencies)
		}
	}

	// Chain a new Append RPC if this one is over a threshold size.
	if aa.checkpoint > appendBufferCutoff ||
		// Or has dependencies which are not a subset of |aa|'s.
		!dependencies.IsSubsetOf(aa.dependencies) ||
		// Or if the requests themselves differ.
		!req.Equal(aa.Request()) {

		aa = s.chainNewAppend(aa, req, dependencies)
	}

	if aa.fb == nil {
		// This is the first time this AsyncAppend is being returned by
		// StartAppend. Initialize its appendBuffer, which also signals that this
		// |aa| has been returned by StartAppend, and that a client may be
		// waiting on its RPC response.
		aa.fb = appendBufferPool.Get().(*appendBuffer)
	}
	return aa
}

// PendingExcept implements the AsyncJournalClient interface.
func (s *AppendService) PendingExcept(except pb.Journal) OpFutures {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out = make(OpFutures, len(s.errs)+len(s.appends))
	for j, err := range s.errs {
		if j != except {
			out[FinishedOperation(err)] = struct{}{}
		}
	}
	for _, aa := range s.appends {
		if aa.Request().Journal != except {
			out[aa] = struct{}{}
		}
	}
	return out
}

// chainNewAppend adds and returns a new AsyncAppend, to be ordered after this one.
func (s *AppendService) chainNewAppend(aa *AsyncAppend, req pb.AppendRequest, dependencies OpFutures) *AsyncAppend {
	// Precondition: aa.mu lock is already held.
	if aa.next != nil {
		panic("aa.next != nil")
	}
	aa.next = &AsyncAppend{
		op:           *NewAsyncOperation(),
		app:          *NewAppender(s.ctx, s.RoutedJournalClient, req),
		dependencies: dependencies,
		mu:           aa.mu,
	}

	s.mu.Lock()
	s.appends[aa.Request().Journal] = aa.next
	s.mu.Unlock()

	return aa.next
}

// AsyncAppend represents an asynchronous Append RPC started and managed by
// an AppendService.
type AsyncAppend struct {
	op  AsyncOperation
	app Appender

	dependencies OpFutures     // Operations which must complete before this one may begin.
	fb           *appendBuffer // Buffer into which writes are queued.
	checkpoint   int64         // Buffer |fb| offset to append through.

	mu   *sync.Mutex  // Shared mutex over all AsyncAppends of the journal.
	next *AsyncAppend // Next ordered AsyncAppend of the journal.
}

// Writer returns a bufio.Writer to which content may be appended. Writer is
// valid for use only until Release is called. Clients may ignore write errors
// of the Writer, preferring to "fire and forget" a sequence of writes which
// could fail: Release will itself Require that no error is set on the Writer.
func (p *AsyncAppend) Writer() *bufio.Writer { return p.fb.buf }

// Require the error to be nil. If Require is called with a non-nil error,
// the error is retained and later returned by Release, in which case it will
// also roll back any writes queued by the caller, aborting the append
// transaction. Require is valid for use only until Release is called.
// Require returns itself, allowing uses like:
//      Require(maybeErrors()).Release()
func (p *AsyncAppend) Require(err error) *AsyncAppend {
	if err != nil && p.op.err == nil {
		p.op.err = err
	}
	return p
}

// Release the AsyncAppend, allowing further writes to queue or for it to be
// dispatched to the brokers. Release first determines whether a previous Require
// failed, or if a Writer error occurred, in which case it will roll back all
// writes queued by the caller, aborting the append transaction, and return the
// non-nil error. A non-nil error is returned if and only if the Append was
// rolled back. Otherwise, the caller may then select on Done to determine when
// the AsyncAppend has committed and its Response may be examined.
func (p *AsyncAppend) Release() error {
	// Require that a bufio.Writer error is not set.
	var _, err = p.fb.buf.Write(nil)
	p.Require(err)

	// Swap and test whether |p.op.err| was set.
	if err, p.op.err = p.op.err, nil; err != nil {
		// rollback in background, as it may block until an underlying disk
		// error is resolved. Note |mu| is still held until rollback completes.
		go p.rollback()
		return err
	}
	p.checkpoint = p.fb.offset + int64(p.fb.buf.Buffered())
	p.mu.Unlock()

	return nil
}

// rollback discards all content written to the Writer and releases the AsyncAppend.
func (p *AsyncAppend) rollback() {
	// flush as |p.checkpoint| may reference still-buffered content.
	retryUntil(p.fb.flush, p.app.Request.Journal, "failed to flush appendBuffer")
	p.fb.offset = p.checkpoint
	retryUntil(p.fb.seek, p.app.Request.Journal, "failed to seek appendBuffer")

	p.mu.Unlock()
}

// Request returns the AppendRequest that was or will be made by this AsyncAppend.
// Request is safe to call at all times.
func (p *AsyncAppend) Request() pb.AppendRequest { return p.app.Request }

// Response returns the AppendResponse from the broker, and may be called only
// after Done selects.
func (p *AsyncAppend) Response() pb.AppendResponse { return p.app.Response }

// Done returns a channel which selects when the AsyncAppend has committed
// or has been aborted along with the AppendService's Context.
func (p *AsyncAppend) Done() <-chan struct{} { return p.op.Done() }

// Err blocks until Done, and returns the final operation error.
func (p *AsyncAppend) Err() error { return p.op.Err() }

// serveAppends executes Append RPCs specified by a (potentially growing) chain
// of ordered AsyncAppends until none remain. Each RPC is retried until
// successful. As each AsyncAppend is completed (up to and including the last
// AsyncAppend of the chain), it is marked as such by setting its |next| to
// itself. serveAppends is a var to facilitate testing.
var serveAppends = func(s *AppendService, aa *AsyncAppend, err error) {
	aa.mu.Lock()

	for {
		// Termination condition: this |aa| can be immediately resolved without
		// blocking, and no further appends are queued.
		if aa.next == nil && aa.fb == nil && aa.dependencies == nil {

			s.mu.Lock()
			delete(s.appends, aa.Request().Journal)
			if err != nil {
				s.errs[aa.Request().Journal] = err
			}
			s.mu.Unlock()

			aa.next = aa // Mark |aa| as completed.
			aa.op.Resolve(err)
			aa.mu.Unlock()
			return
		}

		if aa.next == nil {
			// New appends are still directed to this AsyncAppend. Chain a new one,
			// so that |aa| will no longer be modified even after we release |aa.mu|.
			s.chainNewAppend(aa, aa.Request(), nil)
		}
		aa.mu.Unlock() // Further appends may queue while we dispatch this RPC.

		for op := range aa.dependencies {
			<-op.Done()

			// Error of a dependency permanently fails this AsyncAppend, as well
			// as all queued appends to this journal which follow.
			if err == nil && op.Err() != nil {
				err = errors.WithMessage(op.Err(), "dependency failed")
			}
		}

		// If |aa.fb| is nil, then |aa| was never returned by StartAppend and no
		// client can possibly be waiting on its response. We skip performing
		// an Append RPC altogether in this case.
		if err == nil && aa.fb != nil {
			retryUntil(aa.fb.flush, aa.app.Request.Journal, "failed to flush appendBuffer")

			retryUntil(func() error {
				var _, err2 = io.Copy(&aa.app, io.NewSectionReader(aa.fb.file, 0, aa.checkpoint))
				if err2 == nil {
					err2 = aa.app.Close()
				}

				if err2 == context.Canceled || err2 == context.DeadlineExceeded {
					err = err2
					return nil // Break retry loop.
				} else if err2 != nil {
					aa.app.Reset()
					return err2 // Retry by returning |err2|.
				} else {
					return nil // Success; break loop.
				}
			}, aa.app.Request.Journal, "failed to append to journal")

			releaseFileBuffer(aa.fb)
		}

		aa.mu.Lock()
		aa.op.Resolve(err) // Notify clients & dependent appends of completion.

		// Step |aa| to |aa.next|. While doing so, clear fields of prior |aa|
		// not required to represent its final response. This keeps retained
		// AsyncAppends from pinning other resources from the garbage collector.
		//
		// Also set |aa.next| to itself to denote this AsyncAppend as completed.
		aa, aa.next, aa.dependencies, aa.fb = aa.next, aa, nil, nil
	}
}

// appendBuffer composes a backing File with a bufio.Writer, and additionally
// tracks the offset through which the file is written.
type appendBuffer struct {
	file interface {
		io.ReaderAt
		io.Seeker
		io.Writer
	}
	offset int64
	buf    *bufio.Writer
}

func (fb *appendBuffer) Write(p []byte) (n int, err error) {
	n, err = fb.file.Write(p)
	fb.offset += int64(n)
	return
}

// seek the *os.File to |fb.offset|. The buffer must be empty or seek panics.
func (fb *appendBuffer) seek() error {
	if fb.buf.Buffered() != 0 {
		panic("buffer not empty")
	}
	var _, err = fb.file.Seek(fb.offset, io.SeekStart)
	return err
}

// flush the bufio.Writer. If an error is encountered, flush clears the error
// of the bufio.Writer, allowing its continued use, while preserving remaining
// buffered content not yet flushed to the file.
func (fb *appendBuffer) flush() error {
	var err = fb.buf.Flush()

	if err != nil {
		// NOTE(johnny): the following is definitely a hack, which provides a means
		// to "clear" the error set on a bufio.Writer while preserving its buffered
		// content. It relies on current behavior of bufio.Writer.Reset, Write, and
		// ReadFrom to work properly. Due to the Go 1.0 compatibility guarantee,
		// the behavior is unlikely to change anytime soon.

		var n = fb.buf.Buffered() // Amount of buffered content to recover.

		// Chain a new bufio.Writer which we will wrap the Writer formally known
		// as |fb.buf| around. We must do this because ReadFrom will preemptively
		// Flush again if it reads |n == appendBufferSize| content. Write, however,
		// will not flush if exactly |n == appendBufferSize| is written, so we can
		// daisy-chain a new |fb.buf| to which our old |buf| is guaranteed to flush
		// successfully.
		var buf = fb.buf
		fb.buf = bufio.NewWriterSize(fb, buf.Size())

		// Note Reset clears |buf.err| and leaves |buf.buf| alone, but zeroes |buf.n|.
		// Strip buffer interfaces other than io.Writer.
		buf.Reset(struct{ io.Writer }{fb.buf})

		// Use ReadFrom to "read" |n| bytes into |buf| without mutating its content,
		// effectively restoring the buffer as it was prior to Reset.
		var nn, err2 = buf.ReadFrom(returnNReader{n})

		// Flush to |fb.buf|, which is guaranteed to not itself flush.
		if err2 == nil {
			err2 = buf.Flush()
		}
		if err2 != nil || nn != int64(n) {
			panic(fmt.Sprintf("expected nn (%d) == n (%d) && err == nil: %s", nn, n, err2))
		}
	}
	return err
}

type returnNReader struct{ n int }

func (r returnNReader) Read(p []byte) (int, error) {
	if len(p) < r.n {
		panic("len(p) < r.n")
	}
	return r.n, io.EOF
}

func releaseFileBuffer(fb *appendBuffer) {
	fb.offset = 0
	retryUntil(fb.seek, "", "failed to seek appendBuffer")
	appendBufferPool.Put(fb)
}

var appendBufferPool = sync.Pool{
	New: func() interface{} {
		var fb *appendBuffer

		retryUntil(func() (err error) {
			fb, err = newAppendBuffer()
			return
		}, "", "failed to create appendBuffer")

		return fb
	},
}

func retryUntil(fn func() error, journal pb.Journal, msg string) {
	for attempt := 0; true; attempt++ {
		var err = fn()
		if err == nil {
			return
		}
		var fields = log.Fields{"err": err, "attempt": attempt}
		if journal != "" {
			fields["journal"] = journal
		}
		if attempt != 0 {
			log.WithFields(fields).Warn(msg + " (will retry)")
		}
		time.Sleep(backoff(attempt))
	}
}

var (
	appendBufferSize         = 8 * 1024 // 8KB.
	appendBufferCutoff int64 = 1 << 26  // 64MB.
)
