package flow

import (
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// FlowConsumer is the interface implemented by the flow derivation
// and materialization consumer application runtimes.
type FlowConsumer interface {
	consumer.Store
	// shuffle.Store // TODO(johnny): Move & rename FlowConsumer to avoid import cycle.

	// TODO - move to consumer.Store.
	BuildHints() (recoverylog.FSMHints, error)

	BeginTxn(consumer.Shard) error
	ConsumeMessage(consumer.Shard, message.Envelope, *message.Publisher) error
	FinalizeTxn(consumer.Shard, *message.Publisher) error
	FinishedTxn(consumer.Shard, consumer.OpFuture)

	StartReadingMessages(consumer.Shard, pc.Checkpoint, chan<- consumer.EnvelopeOrError)
	ReplayRange(_ consumer.Shard, _ pb.Journal, begin, end pb.Offset) message.Iterator
}
