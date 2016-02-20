package journal

import (
	"io"
)

// A MarkedReader delegates reads to an underlying reader, and maintains
// |Mark| such that it always points to the next byte to be read.
type MarkedReader struct {
	Mark Mark
	r    io.Reader
}

func NewMarkedReader(mark Mark, r io.Reader) *MarkedReader {
	return &MarkedReader{
		Mark: mark,
		r:    r,
	}
}

func (r *MarkedReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.Mark.Offset += int64(n)
	return n, err
}
