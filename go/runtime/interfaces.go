package runtime

import (
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/shuffle"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// Application is the interface implemented by the flow derivation
// and materialization consumer application runtimes.
type Application interface {
	consumer.Store
	shuffle.Store

	BeginTxn(consumer.Shard) error
	ConsumeMessage(consumer.Shard, message.Envelope, *message.Publisher) error
	FinalizeTxn(consumer.Shard, *message.Publisher) error
	FinishedTxn(consumer.Shard, consumer.OpFuture)

	StartReadingMessages(consumer.Shard, pc.Checkpoint, *flow.Timepoint, chan<- consumer.EnvelopeOrError, bool)
	ReplayRange(_ consumer.Shard, _ pb.Journal, begin, end pb.Offset) message.Iterator
	ReadThrough(pb.Offsets) (pb.Offsets, error)
}
