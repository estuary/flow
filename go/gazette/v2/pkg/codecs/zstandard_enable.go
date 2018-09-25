// +build !nozstd

package codecs

import (
	"io"

	"github.com/DataDog/zstd"
)

func init() {
	zstdNewReader = func(r io.Reader) (io.ReadCloser, error) { return zstd.NewReader(r), nil }
	zstdNewWriter = func(w io.Writer) (io.WriteCloser, error) { return zstd.NewWriter(w), nil }
}
