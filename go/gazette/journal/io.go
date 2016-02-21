package journal

import (
	"io"
	"time"

	log "github.com/Sirupsen/logrus"
)

// Effectively a constant; mutable for test support.
var kRetryReaderErrCooloff = 5 * time.Second

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

// A RetryReader masks encountered errors, transparently handling retries to
// present its clients with an infinite-length bytestream which may block for
// an arbitrarily long period of time, but also never returns an error / EOF.
type RetryReader struct {
	MarkedReader
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
		time.Sleep(kRetryReaderErrCooloff)
		rr.cooloff = false
	}

	if rr.ReadCloser == nil {
		args := ReadArgs{
			Journal:  rr.Mark.Journal,
			Offset:   rr.Mark.Offset,
			Blocking: true,
		}

		var result ReadResult
		result, rr.ReadCloser = rr.getter.Get(args)

		if result.Error != nil {
			log.WithFields(log.Fields{"args": args, "err": result.Error}).Warn("open failed")
			rr.onError(true)
			return 0, nil
		} else if o := rr.Mark.Offset; o != 0 && o != -1 && o != result.Offset {
			// Offset jumps should be very uncommon, but are possible
			// (eg, data could be missing from the middle of a journal).
			log.WithFields(log.Fields{"mark": rr.Mark, "result": result}).Warn("offset jump")
		}
		rr.Mark.Offset = result.Offset
	}

	n, err := rr.MarkedReader.Read(p)

	if err != nil {
		if err != io.EOF {
			log.WithFields(log.Fields{"mark": rr.Mark, "err": err}).Warn("read failed")
		}
		rr.onError(err != io.EOF)
	}
	return n, nil
}

func (rr *RetryReader) onError(shouldCooloff bool) {
	if err := rr.MarkedReader.Close(); err != nil {
		log.WithField("err", err).Warn("marked reader close failed")
	}
	rr.cooloff = shouldCooloff
}
