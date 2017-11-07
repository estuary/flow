package journal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

var (
	ErrExists            = errors.New("journal exists")
	ErrNotBroker         = errors.New("not journal broker")
	ErrNotFound          = errors.New("journal not found")
	ErrNotReplica        = errors.New("not journal replica")
	ErrNotYetAvailable   = errors.New("offset not yet available")
	ErrReplicationFailed = errors.New("replication failed")
	ErrWrongRouteToken   = errors.New("wrong route token")
	ErrWrongWriteHead    = errors.New("wrong write head")

	protocolErrors = []error{
		ErrExists,
		ErrNotBroker,
		ErrNotFound,
		ErrNotReplica,
		ErrNotYetAvailable,
		ErrReplicationFailed,
		ErrWrongRouteToken,
		ErrWrongWriteHead,
	}
)

// Token which describes the ordered set of responsible servers for a Journal:
// the first acts as broker, and the rest serve replications and reads (only).
// Structured as '|'-separated URLs rooting the server's Journal hierarchy.
// Ex: "http://srv-2/a/root|https://srv-1|http://12.34.56.7:8080/other/root".
type RouteToken string

type ReplicateArgs struct {
	Journal Name
	// WriteHead (eg, first byte) of the replicated transaction.
	// Already known and verified by all journal replicas.
	WriteHead int64
	// RouteToken of the transaction, also known and verified by all replicas.
	RouteToken
	// Flags whether replicas should begin a new spool for this transaction.
	NewSpool bool
}

type ReplicateResult struct {
	Error error
	// Iff |Error| is ErrWrongWriteHead, then |ErrorWriteHead| is the replica's
	// own, strictly greater write head.
	ErrorWriteHead int64
	// Set iff |Error| is nil.
	Writer WriteCommitter
}

type ReplicateOp struct {
	ReplicateArgs

	// Channel by which replica returns a ReplicateResult.
	Result chan ReplicateResult `json:"-"`
}

type ReadArgs struct {
	Journal Name
	// Desired offset to begin reading from. Value -1 has special handling, where
	// the read is performed from the current write head. All other positive
	// values specify a desired exact byte offset to read from. If the offset is
	// not available (eg, because it represents a portion of Journal which has
	// been permantently deleted), the broker will return the next available
	// offset. Callers should therefore always inspect the ReadResult Offset.
	Offset int64
	// Whether the operation should block until content becomes available.
	// ErrNotYetAvailable is returned if a non-blocking read has no ready content.
	Blocking bool
	// Context which may cancel or supply a deadline for the operation.
	Context context.Context

	// Deprecated: Server-side support for deadlines will be removed. Use
	// context.WithDeadline instead.
	// The time at which blocking will expire
	Deadline time.Time
}

type ReadResult struct {
	Error error
	// The effective offset of the operation.
	Offset int64
	// Write head at the completion of the operation.
	WriteHead int64
	// RouteToken of the Journal. Set on ErrNotReplica.
	RouteToken
	// Result fragment, set iff |Error| is nil.
	Fragment Fragment
}

type ReadOp struct {
	ReadArgs

	// Channel by which replica returns a ReadResult.
	Result chan ReadResult `json:"-"`
}

type AppendArgs struct {
	Journal Name
	// Content to be appended to |Journal|. The append will consume |Content|
	// until io.EOF, and abort the append (without committing any content)
	// if any other error is returned by |Content.Read()|.
	Content io.Reader
}

type AppendResult struct {
	// Any error that occurred during the append operation (PUT request.)
	Error error
	// Write head at the completion of the operation.
	WriteHead int64
	// RouteToken of the Journal. Set on ErrNotBroker.
	RouteToken
}

type AppendOp struct {
	AppendArgs

	// Channel by which broker returns operation status.
	Result chan AppendResult `json:"-"`
}

// Represents an AppendOp which is being asynchronously executed.
type AsyncAppend struct {
	// Read-only, and valid only after Ready is signaled.
	AppendResult
	// Signaled with the AppendOp has completed.
	Ready chan struct{}
}

// Maps Journal protocol errors into a unique HTTP status code.
// Other errors are mapped into http.StatusInternalServerError.
func StatusCodeForError(err error) int {
	switch err {
	case ErrExists:
		return http.StatusConflict // 409.
	case ErrNotBroker:
		return http.StatusGone // 410.
	case ErrNotFound:
		return http.StatusNotFound // 404.
	case ErrNotReplica:
		return http.StatusTemporaryRedirect // 307.
	case ErrNotYetAvailable:
		return http.StatusRequestedRangeNotSatisfiable // 416.
	case ErrReplicationFailed:
		return http.StatusServiceUnavailable // 503.
	case ErrWrongRouteToken:
		return http.StatusProxyAuthRequired // 407.
	case ErrWrongWriteHead:
		return http.StatusPreconditionFailed // 412.
	default:
		return http.StatusInternalServerError // 500.
	}
}

// Maps a HTTP status code into a correponding Journal protocol error, or nil.
// Unknown status codes are converted into an error.
func ErrorFromResponse(response *http.Response) error {
	switch response.StatusCode {
	case http.StatusPartialContent:
		// Successful Read. Leave response body alone.
		return nil
	case http.StatusNoContent, http.StatusCreated:
		// Successful Append, Replicate, or Create. No body expected.
		_ = response.Body.Close()
		return nil
	default:
	}

	// The response indicates failure. Consume the response body.
	defer response.Body.Close()

	switch response.StatusCode {
	case http.StatusConflict: // 409.
		return ErrExists
	case http.StatusGone: // 410.
		return ErrNotBroker
	case http.StatusNotFound: // 404.
		return ErrNotFound
	case http.StatusTemporaryRedirect: // 307.
		return ErrNotReplica
	case http.StatusRequestedRangeNotSatisfiable: // 416.
		return ErrNotYetAvailable
	case http.StatusServiceUnavailable: // 503.
		return ErrReplicationFailed
	case http.StatusProxyAuthRequired: // 407.
		return ErrWrongRouteToken
	case http.StatusPreconditionFailed: // 412.
		return ErrWrongWriteHead
	default:
		if body, err := ioutil.ReadAll(response.Body); err != nil {
			return err
		} else {
			return fmt.Errorf("%s (%s)", response.Status, string(body))
		}
	}
}
