package journal

import (
	"errors"
	"io"
	"time"
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

type ReplicateArgs struct {
	Journal   Name
	WriteHead int64
	// Token describing the set of servers involved in the transaction.
	RouteToken string
	// Flags whether replicas should begin a new spool for this transaction.
	NewSpool bool
}

type ReplicateResult struct {
	Error error
	// Iff |Error| == ErrWrongWriteHead, then |ErrorWriteHead| is the replica's
	// own, strictly greater write head.
	ErrorWriteHead int64
	// Set iff |Error| == nil.
	Writer WriteCommitter
}

type ReplicateOp struct {
	ReplicateArgs

	// Channel by which replica returns a ReplicateResult.
	Result chan ReplicateResult `json:"-"`
}

type ReadArgs struct {
	Journal Name
	// Offset to begin reading from. Values 0 and -1 have special handling:
	//  * If 0, the read is performed at the first available journal offset.
	//  * If -1, then the read is performed from the current write head.
	// All other values specify an exact byte offset which must be read from.
	Offset int64
	// DEPRICATED.  To be replaced by |Deadline|.  Whether this operation should
	// block until the requested offset becomes available.
	Blocking bool
	// The time at which blocking will expire
	Deadline time.Time
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
