package fragment

import (
	"io"

	"go.gazette.dev/core/broker/protocol"
)

// Fragment wraps the protocol.Fragment type with a nil-able backing local File.
type Fragment struct {
	protocol.Fragment
	// Local uncompressed file of the Fragment, or nil iff the Fragment is remote.
	File File
}

// File is the subset of os.File used in backing Fragments with local files.
type File interface {
	io.ReaderAt
	io.Seeker
	io.WriterAt
	io.Writer
	io.Closer
}
