package journal

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Effectively constants; mutable for test support.
var (
	retryReaderErrCooloff = 5 * time.Second
	timeNow               = time.Now
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
	n, err := r.ReadCloser.Read(p)
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
// (which must wrap this MarkedReader`) but unconsumed from |br|'s buffer.
func (r *MarkedReader) AdjustedMark(br *bufio.Reader) Mark {
	return Mark{
		Journal: r.Mark.Journal,
		Offset:  r.Mark.Offset - int64(br.Buffered()),
	}
}

// A RetryReader masks encountered errors, transparently handling retries to
// present its clients with an infinite-length bytestream which may block for
// an arbitrarily long period of time.
type RetryReader struct {
	MarkedReader
	// Duration RetryReader should block for new content before returning EOF.
	// Specifically, RetryReader will issue reads with server deadline EOFTimeout,
	// and return EOF on Read iff the read is server-closed with no content.
	// If EOFTimeout is zero, EOF is never returned by Read.
	EOFTimeout time.Duration
	// Result of the most recent read.
	Result ReadResult

	getter  Getter
	cooloff bool
}

func NewRetryReader(mark Mark, getter Getter) *RetryReader {
	return &RetryReader{
		MarkedReader: MarkedReader{Mark: mark},
		getter:       getter,
	}
}

func (rr *RetryReader) Read(p []byte) (int, error) {
	if rr.cooloff {
		time.Sleep(retryReaderErrCooloff)
		rr.cooloff = false
	}

	var firstRead bool

	if rr.ReadCloser == nil {
		firstRead = true // First read of the current reader.

		if args, err := rr.open(); err != nil {
			if err == ErrNotFound && args.Offset <= 0 {
				// Initialization case: We're reading from the beginning of a journal
				// which doesn't exist yet.
				if rr.EOFTimeout != 0 {
					return 0, io.EOF
				}
			} else if err != nil {
				log.WithFields(log.Fields{"args": args, "err": rr.Result.Error}).Warn("open failed")
			}
			// Under the io.Reader contract, zero-length reads are allowed.
			// The caller will retry.
			return 0, nil
		}
	}
	n, err := rr.MarkedReader.Read(p)

	if err == io.EOF {
		rr.onError(false)

		if firstRead && rr.EOFTimeout != 0 {
			// We received a server EOF on a deadline request with no content.
			// Pass through the EOF.
			return n, io.EOF
		}
	} else if err != nil {
		rr.onError(true)

		// Log, but mask the error.
		log.WithFields(log.Fields{"mark": rr.Mark, "err": err}).Warn("read failed")
	}
	return n, nil
}

func (rr *RetryReader) open() (ReadArgs, error) {
	var args = ReadArgs{
		Journal:  rr.Mark.Journal,
		Offset:   rr.Mark.Offset,
		Blocking: rr.EOFTimeout == 0,
	}
	if rr.EOFTimeout != 0 {
		args.Deadline = timeNow().Add(rr.EOFTimeout)
	}

	rr.Result, rr.ReadCloser = rr.getter.Get(args)

	if rr.Result.Error != nil {
		rr.onError(true)
		return args, rr.Result.Error
	}

	if o := rr.Mark.Offset; o != 0 && o != -1 && o != rr.Result.Offset {
		// Offset jumps should be very uncommon, but are possible if data has
		// been expunged from the middle of a journal.
		log.WithFields(log.Fields{"mark": rr.Mark, "result": rr.Result}).Warn("offset jump")
	}
	rr.Mark.Offset = rr.Result.Offset
	return args, nil
}

func (rr *RetryReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_SET:
		// |offset| is absolute.
	case os.SEEK_CUR:
		offset = rr.Mark.Offset + offset
	default:
		// SEEK_END is not supported.
		return rr.Mark.Offset, errors.New("invalid whence")
	}

	var delta = offset - rr.Mark.Offset
	rr.Mark.Offset = offset

	if rr.ReadCloser == nil || delta < 0 || offset >= rr.Result.Fragment.End {
		// Seek cannot be satisfied with the open reader. Close to retry.
		rr.onError(false)
		return rr.Mark.Offset, nil
	}

	if _, err := io.CopyN(ioutil.Discard, rr.ReadCloser, delta); err != nil {
		log.WithFields(log.Fields{"delta": delta, "err": err}).Warn("seeking reader")
		return rr.Mark.Offset, rr.Close() // Close to retry.
	}

	return rr.Mark.Offset, nil
}

func (rr *RetryReader) onError(shouldCooloff bool) {
	if err := rr.MarkedReader.Close(); err != nil {
		log.WithField("err", err).Warn("marked reader close failed")
	}
	rr.cooloff = shouldCooloff
}
