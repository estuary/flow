package client

import (
	"bufio"
	"context"
	"errors"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// RetryReader wraps Reader with error handling and retry behavior, as well as
// support for cancellation of an ongoing Read or Seek operation. RetryReader
// is not thread-safe, with one exception: Cancel may be called from one
// goroutine to abort an ongoing Read or Seek call in another.
type RetryReader struct {
	// Context of the RetryReader, which parents the Context provided to
	// underlying *Reader instances.
	Context context.Context
	// Client of the RetryReader.
	Client pb.RoutedJournalClient
	// Reader is the current underlying Reader of the RetryReader. This instance
	// may change many times over the lifetime of a RetryReader, as Read RPCs
	// finish or are cancelled and then restarted.
	Reader *Reader
	// Cancel Read operations of the current *Reader. Notably this will cause an
	// ongoing blocked Read (as well as any future Reads) to return a "Cancelled"
	// error. Restart may be called to re-initialize the RetryReader.
	Cancel context.CancelFunc
}

// NewRetryReader returns a RetryReader initialized with the BrokerClient and ReadRequest.
func NewRetryReader(ctx context.Context, client pb.RoutedJournalClient, req pb.ReadRequest) *RetryReader {
	var rr = &RetryReader{
		Context: ctx,
		Client:  client,
	}
	rr.Restart(req)
	return rr
}

// Journal being read by this RetryReader.
func (rr *RetryReader) Journal() pb.Journal {
	return rr.Reader.Request.Journal
}

// Offset of the next Journal byte to be returned by Read.
func (rr *RetryReader) Offset() int64 {
	return rr.Reader.Request.Offset
}

// Read returns the next bytes of journal content. It will return a non-nil
// error in the following cases:
//  * Cancel is called, or the RetryReader context is cancelled.
//  * The broker returns OFFSET_NOT_YET_AVAILABLE (ErrOffsetNotYetAvailable)
//    for a non-blocking ReadRequest.
//  * An offset jump occurred (ErrOffsetJump), in which case the client
//    should inspect the new Offset and may continue reading if desired.
//  * The broker returns io.EOF upon reaching the requested EndOffset.
// All other errors are retried.
func (rr *RetryReader) Read(p []byte) (n int, err error) {
	for attempt := 0; true; attempt++ {

		if n, err = rr.Reader.Read(p); err == nil {
			return // Success.
		} else if err == io.EOF && rr.Reader.Request.EndOffset != 0 &&
			rr.Reader.Request.Offset >= rr.Reader.Request.EndOffset {
			return // Success (read through requested EndOffset).
		} else if err == ErrOffsetJump {
			return // Note |rr.Reader| is not invalidated by this error.
		}

		// Our Read failed. Since we're a retrying reader, we consume and mask
		// errors (possibly logging a warning), manage our own back-off timer,
		// and restart the stream when ready for another attempt.

		// Restart the Reader, carrying forward fields which continue to apply.
		// Note we're re-using the same context (and we could be racing
		// this restart with a concurrent call to |rr.Cancel|).
		rr.Reader = &Reader{
			Request:  rr.Reader.Request,
			Response: rr.Reader.Response,
			ctx:      rr.Reader.ctx,
			client:   rr.Reader.client,
		}

		switch err {
		case context.DeadlineExceeded, context.Canceled:
			return // Surface to caller.
		case ErrOffsetNotYetAvailable:
			if rr.Reader.Request.Block {
				// |Block| was set after a non-blocking reader was started. Restart in blocking mode.
			} else {
				return // Surface to caller.
			}
		case io.EOF, ErrNotJournalBroker:
			// Suppress logging for expected errors.
		default:
			log.WithFields(log.Fields{
				"journal": rr.Journal(),
				"offset":  rr.Offset(),
				"err":     err,
				"attempt": attempt,
			}).Warn("read failure (will retry)")
		}

		if n != 0 {
			err = nil // Squelch from caller.
			return
		}

		// Wait for a back-off timer, or context cancellation.
		select {
		case <-rr.Reader.ctx.Done():
			return 0, rr.Reader.ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
	panic("not reached")
}

// Seek sets the offset for the next Read. It returns an error if (and only if)
// whence is io.SeekEnd, which is not supported. Where possible Seek will delegate
// to the current Reader, but in most cases a new Read RPC must be started.
func (rr *RetryReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.Reader.Request.Offset + offset
	case io.SeekEnd:
		return rr.Reader.Request.Offset, errors.New("io.SeekEnd whence is not supported")
	default:
		panic("invalid whence")
	}

	if _, err := rr.Reader.Seek(offset, io.SeekStart); err != nil {
		if err != ErrSeekRequiresNewReader {
			log.WithFields(log.Fields{"journal": rr.Journal(), "offset": offset, "err": err}).
				Warn("failed to seek open Reader (will retry)")
		}

		var req = rr.Reader.Request
		req.Offset = offset

		rr.Cancel()
		rr.Restart(req)
	}
	return rr.Reader.Request.Offset, nil
}

// AdjustedOffset delegates to the current Reader's AdjustedOffset.
func (rr *RetryReader) AdjustedOffset(br *bufio.Reader) int64 { return rr.Reader.AdjustedOffset(br) }

// AdjustedSeek sets the offset for the next Read, accounting for buffered data and,
// where possible, accomplishing the AdjustedSeek by discarding from the bufio.Reader.
func (rr *RetryReader) AdjustedSeek(offset int64, whence int, br *bufio.Reader) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.AdjustedOffset(br) + offset
	case io.SeekEnd:
		return rr.AdjustedOffset(br), errors.New("io.SeekEnd whence is not supported")
	default:
		panic("invalid whence")
	}

	var delta = offset - rr.AdjustedOffset(br)

	// Fast path: can we fulfill the seek by discarding a portion of buffered data?
	if delta >= 0 && delta <= int64(br.Buffered()) {
		_, _ = br.Discard(int(delta))
		return offset, nil
	}

	// We must Seek the underlying reader, discarding and resetting the current buffer.
	var n, err = rr.Seek(offset, io.SeekStart)
	br.Reset(rr)
	return n, err
}

// Restart the RetryReader with a new ReadRequest.
// Restart without a prior Cancel will leak resources.
func (rr *RetryReader) Restart(req pb.ReadRequest) {
	var ctx, cancel = context.WithCancel(rr.Context)

	rr.Reader = NewReader(ctx, rr.Client, req)
	rr.Cancel = cancel
}

func backoff(attempt int) time.Duration {
	// The choices of backoff time reflect that we're usually waiting for the
	// cluster to converge on a shared understanding of ownership, and that
	// involves a couple of Nagle-like read delays (~30ms) as Etcd watch
	// updates are applied by participants.
	switch attempt {
	case 0, 1:
		return time.Millisecond * 50
	case 2, 3:
		return time.Millisecond * 100
	case 4, 5:
		return time.Second
	default:
		return 5 * time.Second
	}
}
