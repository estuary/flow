package consumer

import (
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/message"
	"github.com/pippio/gazette/topic"
)

type ShardID int

type Shard interface {
	// The concrete ID of this Shard.
	ID() ShardID

	// A consumer may wish to maintain in-memory state for
	// performance reasons. Examples could include:
	//  * Objects we’re reducing over, for which we wish to avoid
	//    excessive database writes.
	//  * An LRU of "hot" objects we expect to reference again soon.
	// However, to guarantee required transactionality properties,
	// consumers must be careful not to mix states between shards.
	// |Cache| is available to consumers for shard-level isolation
	// of a consumer-specific local memory context.
	Cache() interface{}
	SetCache(interface{})

	// Returns the database of the Shard.
	Database() *rocks.DB

	// Current transaction of the consumer shard. All writes to the
	// database must be issued through the returned WriteBatch.
	Transaction() *rocks.WriteBatch

	// Returns initialized read and write options for the database.
	ReadOptions() *rocks.ReadOptions
	WriteOptions() *rocks.WriteOptions
}

type Consumer interface {
	// The topic this consumer is consuming.
	Topics() []*topic.Description

	// Called when a message becomes available from one of the consumer’s
	// joined topics. If the returned error is non-nil, the Shard is assumed to
	// be in an unhealthy state and will be torn down.
	Consume(message.Message, Shard, topic.Publisher) error

	// Called when a consumer transaction is about to complete. If the Shard
	// Cache() contains any modified state, it must be persisted to Transaction()
	// during this call. As in Consume(), a returned error will result in the
	// tear-down of the Shard.
	Flush(Shard, topic.Publisher) error
}

// Optional Consumer interface for notification of Shard initialization prior
// to an initial Consume. A common use case is to initialize the shard cache.
type ShardIniter interface {
	InitShard(Shard) error
}

// Optional Consumer interface for notification that processing is stopping.
// No Consume or Flush calls will be issued after ShardStopping.
type ShardStopper interface {
	ShardStopping(Shard)
}
