package codecs

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/gzip"
	pb "go.gazette.dev/core/broker/protocol"
)

// Decompressor is a ReadCloser where Close closes and releases Decompressor
// state, but does not Close or affect the underlying Reader.
type Decompressor io.ReadCloser

// Compressor is a WriteCloser where Close closes and releases Compressor
// state, potentially flushing final content to the underlying Writer,
// but does not Close or otherwise affect the underlying Writer.
type Compressor io.WriteCloser

// NewCodecReader returns a Decompressor of the Reader encoded with CompressionCodec.
func NewCodecReader(r io.Reader, codec pb.CompressionCodec) (Decompressor, error) {
	switch codec {
	case pb.CompressionCodec_NONE, pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION:
		return ioutil.NopCloser(r), nil
	case pb.CompressionCodec_GZIP:
		return gzip.NewReader(r)
	case pb.CompressionCodec_SNAPPY:
		return ioutil.NopCloser(snappy.NewReader(r)), nil
	case pb.CompressionCodec_ZSTANDARD:
		return zstdNewReader(r)
	default:
		return nil, fmt.Errorf("unsupported codec %s", codec.String())
	}
}

// NewCodecWriter returns a Compressor wrapping the Writer encoding with CompressionCodec.
func NewCodecWriter(w io.Writer, codec pb.CompressionCodec) (Compressor, error) {
	switch codec {
	case pb.CompressionCodec_NONE:
		return nopWriteCloser{w}, nil
	case pb.CompressionCodec_GZIP:
		return &GzipMultiMemberWriter{w: w}, nil
	case pb.CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION:
		return gzip.NewWriter(w), nil
	case pb.CompressionCodec_SNAPPY:
		return snappy.NewBufferedWriter(w), nil
	case pb.CompressionCodec_ZSTANDARD:
		return zstdNewWriter(w)
	default:
		return nil, fmt.Errorf("unsupported codec %s", codec.String())
	}
}

// GzipMultiMemberWriter allows for batching multiple writes into a single gzip
// member, which are concatenated into a single file per RFC 1952. Members are
// terminated by calling Close, with a new gzip writer initialized on the next
// Write.
type GzipMultiMemberWriter struct {
	w  io.Writer
	gz *gzip.Writer
}

func (gzb *GzipMultiMemberWriter) Write(p []byte) (n int, err error) {
	if gzb.gz == nil {
		gzb.gz = gzip.NewWriter(gzb.w)
	}

	return gzb.gz.Write(p)
}

func (gzb *GzipMultiMemberWriter) Close() error {
	if gzb.gz == nil {
		return nil
	} else if err := gzb.gz.Close(); err != nil {
		return err
	}
	gzb.gz = nil

	return nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

var (
	zstdNewReader = func(io.Reader) (io.ReadCloser, error) {
		return nil, fmt.Errorf("ZSTANDARD was not enabled at compile time")
	}
	zstdNewWriter = func(io.Writer) (io.WriteCloser, error) {
		return nil, fmt.Errorf("ZSTANDARD was not enabled at compile time")
	}
)
