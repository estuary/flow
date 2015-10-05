package journal

import (
	"errors"
	"io"
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
	Journal   Name
	WriteHead int64
	// Token describing the set of servers involved in the transaction.
	RouteToken string
	// Flags whether replicas should begin a new spool for this transaction.
	NewSpool bool
	// Channel by which replica returns a ReplicateResult.
	Result chan ReplicateResult
}

type ReplicateResult struct {
	Error error
	// Iff |Error| == ErrWrongWriteHead, then |ErrorWriteHead| is the replica's
	// own, strictly greater write head.
	ErrorWriteHead int64
	// Set iff |Error| == nil.
	Writer WriteCommitter
}

type ReadOp struct {
	Journal Name
	// Offset to begin reading from. Values 0 and -1 have special handling:
	//  * If 0, the read is performed at the first available journal offset.
	//  * If -1, then the read is performed from the current write head.
	// All other values specify an exact byte offset which must be read from.
	Offset int64
	// Whether this operation should block until the requested offset
	// becomes available.
	Blocking bool
	// Channel by which replica returns a ReadResult.
	Result chan ReadResult
}

type ReadResult struct {
	Error error
	// The effective |Offset| of the operation. It will differ from
	// ReadOp.Offset only for special requested values 0 and -1:
	//  * If 0, |Offset| reflects the first available offset.
	//  * If -1, |Offset| reflects the write head at operation start.
	Offset int64
	// Write head at the completion of the operation.
	WriteHead int64
	// Set iff |Error| == nil.
	Fragment Fragment
}

type AppendOp struct {
	Journal Name
	// Content to be appended to |Journal|. The append will consume |Content|
	// until io.EOF, and abort the append (without committing any content)
	// if any other error is returned by |Content.Read()|.
	Content io.Reader
	// Channel by which broker returns operation status.
	Result chan error
}
