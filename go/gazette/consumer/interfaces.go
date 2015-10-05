package topic

import (
	"github.com/tecbot/gorocksdb"
)

type Consumer interface {
	// The topic this consumer is consuming.
	Topics() []*TopicDescription

	// Called when a message becomes available from one of the consumer’s
	// joined topics.
	Consume(msg interface{}, from *TopicDescription, context *ConsumerContext)

	// Called when a consumer transaction is about to complete. If
	// |context.Cache| contains any modified state, it must be
	// persisted to |context.Transaction| after this call.
	Flush(context *ConsumerContext)
}

// Interface for consumers that wish to perform initialization due to new
// ConsumerContexts, prior to an initial Consume() call. This may be used for
// bootstrap or recovery of cached in-memory state.
type ContextIniter interface {
	InitContext(context *ConsumerContext)
}

// Interface for consumers using concurrency other than one.
type ConcurrentExecutor interface {
	Concurrency() int
}

// Interface for consumers using database options other than the default.
type RocksDBOptioner interface {
	RocksDBOptions() *gorocksdb.Options
}

// Interface for consumers using database write options other than the default.
type RocksWriteOptioner interface {
	RocksWriteOptions() *gorocksdb.WriteOptions
}
