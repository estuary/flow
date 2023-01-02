package flow

import (
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
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
// This is copied from gazette's `client` package.
type OpFuture interface {
	// Done selects when operation background execution has finished.
	Done() <-chan struct{}
	// Err blocks until Done() and returns the final error of the OpFuture.
	Err() error
}
