package consumer

import (
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/topic"
)

// ShardID uniquely identifies a specific Shard. A ShardID must be consistent
// across processes for the entire duration of the Consumer lifetime.
type ShardID string

func (id ShardID) String() string {
	return string(id)
}

type Shard interface {
	// The concrete ID of this Shard.
	ID() ShardID
	// The consumed Partition of this Shard.
	Partition() topic.Partition

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

	// Current Transaction of the consumer shard. All writes issued through
	// Transaction will commit atomically and be check-pointed with consumed
	// Journal offsets. This provides exactly-once processing of Journal content
	// (though note that Gazette is itself an at-least once system, and Journal
	// writes themselves could be duplicated). Writes may be done directly to
	// the database, in which case they will be applied at-least once (for
	// example, because a Shard is recovered to a state after a write was applied
	// but before corresponding Journal offsets were written).
	Transaction() *rocks.WriteBatch

	// Returns initialized read and write options for the database.
	ReadOptions() *rocks.ReadOptions
	WriteOptions() *rocks.WriteOptions
}

type Consumer interface {
	// Topics this Consumer is consuming.
	Topics() []*topic.Description

	// Called when a message becomes available from one of the consumer’s
	// joined topics. If the returned error is non-nil, the Shard is assumed to
	// be in an unhealthy state and will be torn down.
	Consume(topic.Envelope, Shard, *topic.Publisher) error

	// Called when a consumer transaction is about to complete. If the Shard
	// Cache() contains any modified state, it must be persisted to Transaction()
	// during this call. As in Consume(), a returned error will result in the
	// tear-down of the Shard.
	Flush(Shard, *topic.Publisher) error
}

// Optional Consumer interface for notification of Shard initialization prior
// to an initial Consume. A common use case is to initialize the shard cache.
type ShardIniter interface {
	InitShard(Shard) error
}

// Optional Consumer interface for notification the Shard is no longer being
// processed by this Consumer. No further Consume or Flush calls will occur,
// nor will further writes to the recovery log. A common use case is to hint
// to the Consumer that external resources or connections associated with the
// Shard should be released.
type ShardHalter interface {
	HaltShard(Shard)
}

// Optional Consumer interface for customization of Shard database options
// prior to initial open.
type OptionsIniter interface {
	InitOptions(*rocks.Options)
}
