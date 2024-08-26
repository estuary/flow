package flow

import (
	"context"
	"errors"
	"fmt"

	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Re-export common types of the Gazette broker and consumer protocols,
// so that this package can be used as a drop-in for the 80% of cases where
// only these types are needed, without requiring a new dependency on Gazette.

type Endpoint = pb.Endpoint
type Journal = pb.Journal
type JournalSpec = pb.JournalSpec
type Label = pb.Label
type LabelSelector = pb.LabelSelector
type LabelSet = pb.LabelSet
type Offset = pb.Offset
type Offsets = pb.Offsets

type Checkpoint = pc.Checkpoint
type ShardID = pc.ShardID
type ShardSpec = pc.ShardSpec

var MustLabelSet = pb.MustLabelSet

// OpFuture represents an operation which is executing in the background. The
// operation has completed when Done selects. Err may be invoked to determine
// whether the operation succeeded or failed.
type OpFuture = client.OpFuture

// AsyncOperation is a simple, minimal implementation of the OpFuture interface.
type AsyncOperation = client.AsyncOperation

// NewAsyncOperation returns a new AsyncOperation.
var NewAsyncOperation = client.NewAsyncOperation

// FinishedOperation is a convenience that returns an already-resolved AsyncOperation.
var FinishedOperation = client.FinishedOperation

// RunAsyncOperation invokes the given function asynchronously and returns
// an OpFuture which will resolve with its completion or panic.
func RunAsyncOperation(fn func() error) OpFuture {
	var op = NewAsyncOperation()

	go func(op *AsyncOperation) {
		defer func() {
			if op != nil {
				op.Resolve(fmt.Errorf("operation had an internal panic"))
			}
		}()

		op.Resolve(fn())
		op = nil
	}(op)

	return op
}

// UnwrapGRPCError maps an error from Recv() or RecvMsg() into a more-canonical representation, by:
// * Mapping cancellation or deadline-exceeded into canonical `context` errors.
// * Unwrapping an Internal or Unknown status code into its contained error message.
func UnwrapGRPCError(err error) error {
	var status, ok = status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Internal, codes.Unknown:
		return errors.New(status.Message())
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	default:
		return err
	}
}
