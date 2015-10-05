package gazette

import (
	"errors"
	"io"
	"net/http"
)

var (
	ErrNotYetAvailable   = errors.New("offset not yet available")
	ErrReplicationFailed = errors.New("replication failed")
	ErrWrongJournal      = errors.New("wrong journal")
	ErrWrongWriteHead    = errors.New("wrong write head")
	ErrWrongRouteToken   = errors.New("wrong route token")
	ErrNotBroker         = errors.New("not journal broker")
	ErrNotReplica        = errors.New("not journal replica")
)

type ReplicateOp struct {
	Journal    string
	RouteToken string
	WriteHead  int64
	NewSpool   bool

	Result chan ReplicateResult
}

type ReplicateResult struct {
	Error          error
	ErrorWriteHead int64

	Writer WriteCommitter
}

type WriteCommitter interface {
	Write([]byte) (int, error)

	Commit(delta int64) error
}

type ReadOp struct {
	Journal  string
	Offset   int64
	Blocking bool

	Result chan ReadResult
}

type ReadResult struct {
	Error    error
	Fragment Fragment
}

type AppendOp struct {
	Journal string
	Content io.Reader

	Result chan error
}

func ResponseCodeForError(err error) int {
	switch err {
	case ErrNotYetAvailable:
		return http.StatusRequestedRangeNotSatisfiable
	case ErrNotReplica:
		return http.StatusNotFound
	case ErrNotBroker:
		return http.StatusBadGateway
	default:
		// Everything else.
		return http.StatusInternalServerError
	}
}
