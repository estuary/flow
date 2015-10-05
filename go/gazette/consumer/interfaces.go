package consumer

import (
	"github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

type Consumer interface {
	// The topic this consumer is consuming.
	Topics() []*topic.Description

	// Called when a message becomes available from one of the consumer’s
	// joined topics.
	Consume(msg message.Message, from *topic.Description, context *Context)

	// Called when a consumer transaction is about to complete. If
	// |context.Cache| contains any modified state, it must be
	// persisted to |context.Transaction| after this call.
	Flush(context *Context)
}

// Interface for consumers that wish to perform initialization due to new
// ConsumerContexts, prior to an initial Consume() call. This may be used for
// bootstrap or recovery of cached in-memory state.
type ContextIniter interface {
	InitContext(context *Context)
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
