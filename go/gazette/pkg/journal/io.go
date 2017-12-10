package journal

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"time"

	log "github.com/sirupsen/logrus"
)

// A MarkedReader delegates reads to an underlying reader, and maintains
// |Mark| such that it always points to the next byte to be read.
type MarkedReader struct {
	Mark Mark
	io.ReadCloser
}

func NewMarkedReader(mark Mark, r io.ReadCloser) *MarkedReader {
	return &MarkedReader{
		Mark:       mark,
		ReadCloser: r,
	}
}

func (r *MarkedReader) Read(p []byte) (int, error) {
	var n, err = r.ReadCloser.Read(p)
	r.Mark.Offset += int64(n)
	return n, err
}

func (r *MarkedReader) Close() error {
	var rc io.ReadCloser
	rc, r.ReadCloser = r.ReadCloser, nil

	if rc != nil {
		return rc.Close()
	}
	return nil
}

// AdjustedMark returns the current Mark adjusted for content read by |br|
// (which must wrap this MarkedReader) but unconsumed from |br|'s buffer.
func (r *MarkedReader) AdjustedMark(br *bufio.Reader) Mark {
	return Mark{
		Journal: r.Mark.Journal,
		Offset:  r.Mark.Offset - int64(br.Buffered()),
	}
}

// RetryReader wraps a Getter and MarkedReader to provide callers with a
// long-lived journal reader. RetryReader transparently handles and retries
// errors, and will block as needed to await new journal content.
type RetryReader struct {
	// MarkedReader manages the current reader and offset tracking.
	MarkedReader
	// Whether read operations should block (the default). If Blocking is false,
	// than Read operations may return ErrNotYetAvailable.
	Blocking bool
	// LastResult retains the result of the last journal read operation.
	// Callers may access it to inspect metadata returned by the broker.
	// It may be invalidated on every Read call.
	LastResult ReadResult
	// Getter against which to perform read operations.
	Getter Getter
	// Context to use in read operations issued to Getter.
	Context context.Context
}

// Deprecated: Use NewRetryReaderContext instead.
// NewRetryReader returns a RetryReader at |mark|, using the provided
// |getter| for all operations.
func NewRetryReader(mark Mark, getter Getter) *RetryReader {
	return NewRetryReaderContext(context.TODO(), mark, getter)
}

// NewRetryReaderContext returns a RetryReader at |mark|, using the provided
// |getter| and |ctx| for all operations.
func NewRetryReaderContext(ctx context.Context, mark Mark, getter Getter) *RetryReader {
	return &RetryReader{
		MarkedReader: MarkedReader{Mark: mark},
		Blocking:     true,
		Getter:       getter,
		Context:      ctx,
	}
}

// Read returns the next available bytes of journal content, retrying as
// required retry errors or await content to be written. Read will return
// a non-nil error in the following cases:
//  * If the RetryReader context is cancelled.
//  * If Blocking is false, and ErrNotYetAvailable is returned by the broker.
// All other errors are retried.
func (rr *RetryReader) Read(p []byte) (n int, err error) {
	for i := 0; true; i++ {

		// Handle a previously encountered error. Since we're a retrying reader,
		// we consume and mask errors (possibly logging a warning), and manage
		// our own back-off timer.
		if err != nil {
			switch err {
			case ErrNotYetAvailable:
				if !rr.Blocking {
					return
				} else {
					// This RetryReader was in non-blocking mode but has since switched
					// to blocking. Ignore this error and retry as a blocking operation.
				}
			case io.EOF, context.DeadlineExceeded, context.Canceled:
				// Suppress logging for expected errors.
			case ErrNotFound:
				if rr.MarkedReader.Mark.Offset <= 0 {
					// Initialization case: We're reading from the beginning of a journal
					// which doesn't exist yet.
					break
				}
				fallthrough
			default:
				if rr.Context.Err() == nil {
					log.WithFields(log.Fields{"mark": rr.MarkedReader.Mark, "err": err}).
						Warn("Read failure (will retry)")
				}
			}

			// Wait for a back-off timer, or context cancellation.
			select {
			case <-rr.Context.Done():
				return 0, rr.Context.Err()
			case <-time.After(backoff(i)):
			}
		}

		if rr.MarkedReader.ReadCloser == nil {
			var args = ReadArgs{
				Journal:  rr.MarkedReader.Mark.Journal,
				Offset:   rr.MarkedReader.Mark.Offset,
				Blocking: rr.Blocking,
				Context:  rr.Context,
			}

			rr.LastResult, rr.MarkedReader.ReadCloser = rr.Getter.Get(args)
			if n, err = 0, rr.LastResult.Error; err != nil {
				rr.MarkedReader.ReadCloser = nil
				continue
			}

			if o := rr.MarkedReader.Mark.Offset; o != 0 && o != -1 && o != rr.LastResult.Offset {
				// Offset jumps should be uncommon, but are possible if data has
				// been removed from the middle of a journal.
				log.WithFields(log.Fields{"mark": rr.MarkedReader.Mark, "result": rr.LastResult}).
					Warn("offset jump")
			}
			rr.MarkedReader.Mark.Offset = rr.LastResult.Offset
		}

		if n, err = rr.MarkedReader.Read(p); err == nil {
			return
		} else {
			// Close to nil rr.MarkedReader.ReadCloser.
			rr.MarkedReader.Close()

			if n != 0 {
				// If data was returned with the error, squash |err|.
				if err != io.EOF {
					log.WithFields(log.Fields{"err": err, "n": n}).Warn("data read error (will retry)")
				}
				err = nil
				return
			}
		}
	}
	panic("not reached")
}

// Seek sets the offset for the next Read. It returns an error if (and only if)
// |whence| is io.SeekEnd, which is not supported.
func (rr *RetryReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		// |offset| is already absolute.
	case io.SeekCurrent:
		offset = rr.MarkedReader.Mark.Offset + offset
	default:
		return rr.MarkedReader.Mark.Offset, errors.New("io.SeekEnd whence is not supported")
	}

	var delta = offset - rr.MarkedReader.Mark.Offset

	if rr.MarkedReader.ReadCloser == nil || delta < 0 || offset >= rr.LastResult.Fragment.End {
		// Seek cannot be satisfied with the open reader.
		rr.MarkedReader.Close()
	} else if _, err := io.CopyN(ioutil.Discard, rr.MarkedReader.ReadCloser, delta); err != nil {
		log.WithFields(log.Fields{"delta": delta, "err": err}).Warn("seeking reader")
		rr.MarkedReader.Close()
	}

	rr.MarkedReader.Mark.Offset = offset
	return offset, nil
}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0, 1:
		return 0
	case 2:
		return time.Millisecond * 5
	case 3, 4, 5:
		return time.Second * time.Duration(attempt-1)
	default:
		return 5 * time.Second
	}
}
