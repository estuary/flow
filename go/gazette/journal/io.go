package gazette

import (
	"io"
)

type boundedReaderAt struct {
	F                 io.ReaderAt
	Remainder, Offset int64
}

func (r *boundedReaderAt) Read(p []byte) (n int, err error) {
	if r.Remainder <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.Remainder {
		p = p[0:r.Remainder]
	}
	n, err = r.F.ReadAt(p, r.Offset)
	r.Remainder -= int64(n)
	r.Offset += int64(n)
	return
}
