package journal

import (
	"io"
	"net/http"
	"net/url"
)

//go:generate mockery -inpkg -name=Doer
//go:generate mockery -inpkg -name=FragmentFile
//go:generate mockery -inpkg -name=Getter
//go:generate mockery -inpkg -name=Header
//go:generate mockery -inpkg -name=Writer

// A typed journal name. By convention, journals are named using a forward-
// slash notation which captures their hierarchical relationships into
// organizations, topics and partitions. For example, a complete Name might be:
// "company-journals/interesting-topic/part-1234"
type Name string

func (n Name) String() string {
	return string(n)
}

// A Mark references a specific |Offset| within a |Journal|.
type Mark struct {
	Journal Name
	Offset  int64
}

func NewMark(name Name, offset int64) Mark {
	return Mark{name, offset}
}

// A Writer allows for append-only writes to a named journal.
type Writer interface {
	// Appends |buffer| to |journal|. Either all of |buffer| is written, or none
	// of it is. Returns a Promise which is resolved when the write has been
	// fully committed.
	Write(journal Name, buffer []byte) (*AsyncAppend, error)

	// Appends |r|'s content to |journal|, by reading until io.EOF. Either all of
	// |r| is written, or none of it is. Returns a Promise which is resolved when
	// the write has been fully committed.
	ReadFrom(journal Name, r io.Reader) (*AsyncAppend, error)
}

// Performs a Gazette GET operation.
type Getter interface {
	Get(args ReadArgs) (ReadResult, io.ReadCloser)
}

// Performs a Gazette HEAD operation.
type Header interface {
	Head(args ReadArgs) (result ReadResult, fragmentLocation *url.URL)
}

// Performs a Gazette POST operation.
type Creator interface {
	Create(journal Name) error
}

// Provides low-level routing and access to a Gazette service, suitable for
// proxying requests and modeled on http.Client. The client will perform
// journal-based routing to the appropriate Gazette instance. See gazette.Client.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Composes Creator, Getter, Header, and Writer.
type Client interface {
	Creator
	Getter
	Header
	Writer
}

// A Replicator is able to serve a ReplicateOp. It may be backed by a local
// Spool, or by a remote Gazette process.
type Replicator interface {
	Replicate(op ReplicateOp)
}

// A WriteCommitter extends Writer with a protocol for committing those writes.
type WriteCommitter interface {
	io.Writer
	// Commits the first |count| bytes of previous Write([]byte) content.
	Commit(count int64) error
}

// FragmentPersister accepts completed local fragment spools, which should
// be persisted to long-term storage. See |gazette.Persister|.
type FragmentPersister interface {
	Persist(Fragment)
}

// Portions of os.File interface used by Fragment. An interface is used
// (rather than directly using *os.File) in support of test mocks.
type FragmentFile interface {
	Close() error
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Seek(offset int64, whence int) (int64, error)
	Fd() uintptr
	Write(p []byte) (n int, err error)
}
