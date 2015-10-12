package journal

import (
	"io"
)

// A BoundedReaderAt implements an io.Reader reading from a specified range
// of the underlying io.ReaderAt.
type BoundedReaderAt struct {
	ReaderAt          io.ReaderAt
	Remainder, Offset int64
}

func NewBoundedReaderAt(f io.ReaderAt, remainder, offset int64) *BoundedReaderAt {
	return &BoundedReaderAt{f, remainder, offset}
}

func (r *BoundedReaderAt) Read(p []byte) (n int, err error) {
	if r.Remainder <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.Remainder {
		p = p[0:r.Remainder]
	}
	n, err = r.ReaderAt.ReadAt(p, r.Offset)
	r.Remainder -= int64(n)
	r.Offset += int64(n)
	return
}

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
