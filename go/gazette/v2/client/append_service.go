package client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

// AppendService batches, dispatches, and (if needed) retries asynchronous
// Append RPCs. Use of an AppendService is appropriate for clients who make
// large numbers of small writes to a Journal, and where those writes may be
// pipelined and batched to amortize the cost of broker Append RPCs. It may
// also simplify implementations for clients who would prefer to simply have
// writes block until successfully committed, as opposed to handling errors
// and retries themselves. AppendService implements the AsyncJournalClient
// interface.
type AppendService struct {
	pb.RoutedJournalClient
	ctx     context.Context
	appends map[pb.Journal]*AsyncAppend
	mu      sync.Mutex
}

// NewAppendService returns an AppendService with the provided Context and BrokerClient.
func NewAppendService(ctx context.Context, client pb.RoutedJournalClient) *AppendService {
	return &AppendService{
		ctx:                 ctx,
		RoutedJournalClient: client,
		appends:             make(map[pb.Journal]*AsyncAppend),
	}
}

// AsyncJournalClient composes a RoutedJournalClient with an API for performing
// asynchronous Append operations.
type AsyncJournalClient interface {
	pb.RoutedJournalClient

	// StartAppend begins a new asynchronous Append RPC. The caller holds exclusive access
	// to the returned AsyncAppend, and must then:
	//  * Write content to its Writer
	//  * Optionally Require that one or more errors are nil.
	//  * Release the AsyncAppend, allowing queued writes to commit or,
	//    if an error occurred, to roll back.
	//
	// For performance reasons, an Append will often be batched with other Appends
	// dispatched to this AppendService, and note the Response.Fragment will reflect
	// the entire batch written to the broker. In all cases, relative order of
	// Appends is preserved. One or more dependencies may optionally be supplied.
	// The Append RPC will not begin until all such dependencies have committed.
	// Dependencies must be ordered on applicable Journal name or StartAppend panics.
	// They must also be AsyncAppends returned by this client, and not another.
	// StartAppend may retain the slice, and it must not be subsequently modified.
	StartAppend(journal pb.Journal, dependencies ...*AsyncAppend) *AsyncAppend

	// PendingExcept returns a snapshot of the AsyncAppends being evaluated for all
	// Journals _other than_ |except|, ordered on Journal. It can be used to build
	// "barriers" which ensure that all pending writes commit prior to the
	// commencement of a write which is about to be issued. Eg, given:
	//
	//   var aa = as.StartAppend("target", as.PendingExcept("target")...)
	//   aa.Writer().WriteString("checkpoint")
	//   aa.Release()
	//
	// All ongoing appends to journals other than "target" are guaranteed to commit
	// before an Append RPC is begun which writes "checkpoint" to journal "target".
	// PendingExcept("") returns all pending AsyncAppends.
	PendingExcept(journal pb.Journal) []*AsyncAppend
}

// StartAppend implements the AsyncJournalClient interface.
func (s *AppendService) StartAppend(name pb.Journal, dependencies ...*AsyncAppend) *AsyncAppend {
	// Fetch the current AsyncAppend for |name|, or start one if none exists.
	s.mu.Lock()
	var aa, ok = s.appends[name]

	if !ok {
		aa = &AsyncAppend{
			app:          *NewAppender(s.ctx, s.RoutedJournalClient, pb.AppendRequest{Journal: name}),
			dependencies: dependencies,
			commitCh:     make(chan struct{}),
			mu:           new(sync.Mutex),
		}
		s.appends[name] = aa
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
		go serveAppends(s, aa)
	}

	for aa.next != nil {
		aa = aa.next // Follow the chain to the current AsyncAppend.
	}

	if aa == tombstoneAsyncAppend {
		// While we were waiting for |aa.mu|, the serveAppends service loop for this
		// AsyncAppend exited (and it was cleared from |s.appends|). Try again.
		return s.StartAppend(name, dependencies...)
	}

	if aa.checkpoint > appendBufferCutoff || !isSubset(dependencies, aa.dependencies) {
		// We must chain a new Append RPC, ordered after this one.
		aa = s.chainNewAppend(aa, dependencies)
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
func (s *AppendService) PendingExcept(except pb.Journal) []*AsyncAppend {
	s.mu.Lock()
	var out = make([]*AsyncAppend, 0, len(s.appends))
	for _, aa := range s.appends {
		if aa.Request().Journal != except {
			out = append(out, aa)
		}
	}
	s.mu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].Request().Journal < out[j].Request().Journal
	})
	return out
}

// WaitForPendingAppends blocks until all |pending| AsyncAppends have completed.
func WaitForPendingAppends(pending []*AsyncAppend) {
	for _, aa := range pending {
		<-aa.Done()
	}
}

// chainNewAppend adds and returns a new AsyncAppend, to be ordered after this one.
func (s *AppendService) chainNewAppend(aa *AsyncAppend, dependencies []*AsyncAppend) *AsyncAppend {
	// Precondition: aa.mu lock is already held.
	if aa.next != nil {
		panic("aa.next != nil")
	}
	aa.next = &AsyncAppend{
		app:          *NewAppender(s.ctx, s.RoutedJournalClient, aa.Request()),
		dependencies: dependencies,
		commitCh:     make(chan struct{}),
		mu:           aa.mu,
	}

	s.mu.Lock()
	s.appends[aa.Request().Journal] = aa.next
	s.mu.Unlock()

	return aa.next
}

// AsyncAppend is an asynchronous Append RPC.
type AsyncAppend struct {
	app Appender

	dependencies []*AsyncAppend // Appends which must commit before this one may begin.
	commitCh     chan struct{}  // Closed to signal AsyncAppend has committed.
	fb           *appendBuffer  // Buffer into which writes are queued.
	checkpoint   int64          // Buffer |fb| offset to append through.
	err          error          // Retained Require(error) or aborting Context error.

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
// Require returns itself, allowing uses like `Require(maybeErrors()).Release()`
func (p *AsyncAppend) Require(err error) *AsyncAppend {
	if err != nil && p.err == nil {
		p.err = err
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

	// Swap and test whether |p.err| was set.
	if err, p.err = p.err, nil; err != nil {
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
func (p *AsyncAppend) Done() <-chan struct{} { return p.commitCh }

// Err returns nil if Done is not yet closed, or the AsyncAppend committed.
// Otherwise, this AsyncAppend was aborted along with the AppendService Context,
// and Err returns the causal context error (Cancelled or DeadlineExceeded).
func (p *AsyncAppend) Err() error {
	select {
	case <-p.Done():
		return p.err
	default:
		return nil
	}
}

// serveAppends executes Append RPCs specified by a (potentially growing) chain
// of ordered AsyncAppends. Each RPC is retried until successful. Upon reaching
// the end of the chain, serveAppends marks its exit with tombstoneAsyncAppend
// and halts. serveAppends is a var to facilitate testing.
var serveAppends = func(s *AppendService, aa *AsyncAppend) {
	aa.mu.Lock()

	for {
		// Termination condition: this |aa| can be immediately resolved without
		// blocking, and no further appends are queued.
		if aa.next == nil && aa.fb == nil && aa.dependencies == nil {

			s.mu.Lock()
			delete(s.appends, aa.Request().Journal)
			s.mu.Unlock()

			aa.next = tombstoneAsyncAppend
			close(aa.commitCh)
			aa.mu.Unlock()
			return
		}

		if aa.next == nil {
			// New appends are still directed to this AsyncAppend. Chain a new one,
			// so that |aa| will no longer be modified even after we release |aa.mu|.
			s.chainNewAppend(aa, nil)
		}
		aa.mu.Unlock() // Further appends may queue while we dispatch this RPC.

		for _, dep := range aa.dependencies {
			<-dep.Done()

			if dep.Err() != nil && aa.app.ctx.Err() == nil {
				// This can happen only if |dep| and |aa| were created by different
				// AppendServices having differing contexts, which is disallowed.
				panic("dependency Err() != nil, but our own context.Err == nil")
			}
		}

		// If |aa.fb| is nil, then |aa| was never returned by StartAppend and no
		// client can possibly be waiting on its response. We skip performing
		// an Append RPC altogether in this case.

		if aa.fb != nil {
			retryUntil(aa.fb.flush, aa.app.Request.Journal, "failed to flush appendBuffer")

			retryUntil(func() error {
				var _, err = io.Copy(&aa.app, io.NewSectionReader(aa.fb.file, 0, aa.checkpoint))
				if err == nil {
					err = aa.app.Close()
				}

				if err == context.Canceled || err == context.DeadlineExceeded {
					aa.err = err // Retain for Err to return.
					return nil   // Break retry loop.
				} else if err != nil {
					aa.app.Reset()
					return err // Retry by returning |err|.
				} else {
					return nil // Success; break loop.
				}
			}, aa.app.Request.Journal, "failed to append to journal")
		}

		close(aa.commitCh) // Notify clients & dependent appends of completion.

		if aa.fb != nil {
			releaseFileBuffer(aa.fb)
		}

		aa.mu.Lock()
		aa = aa.next
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

func isSubset(a, b []*AsyncAppend) bool {
	var ai, bi, la, lb = 0, 0, len(a), len(b)

	for ai != la && bi != lb {
		if aj := a[ai].Request().Journal; ai != 0 && aj <= a[ai-1].Request().Journal {
			panic("[]*AsyncAppend not ordered")
		} else if bj := b[bi].Request().Journal; aj < bj {
			return false // aj cannot be in |b|.
		} else if bj < aj {
			bi++
		} else if a[ai] != b[bi] {
			return false // Same journals, but different write operations.
		} else {
			ai++
			bi++
		}
	}
	return ai == la // Were all items in |a| matched?
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
		var fields = log.Fields{"err": err}
		if journal != "" {
			fields["journal"] = journal
		}
		log.WithFields(fields).Error(msg + " (will retry)")
		time.Sleep(backoff(attempt))
	}
}

var (
	appendBufferSize         = 8 * 1024 // 8KB.
	appendBufferCutoff int64 = 1 << 26  // 64MB.

	// tombstoneAsyncAppend is chained as the next AsyncAppend to mark that the
	// journal's service loop has finished processing the chain and halted.
	// StartAppend uses tombstoneAsyncAppend to detect that the AsyncAppend it has
	// fetched is no longer being served, and the operation should be restarted.
	tombstoneAsyncAppend = &AsyncAppend{}
)
