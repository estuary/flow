// Package consumer is a library for distributed, stateful consumption of Gazette journals.
package consumer

import (
	"context"

	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// Shard is the processing context of a ShardSpec which is assigned to the
// local consumer process.
type Shard interface {
	// Context of this shard. Notably, the Context will be cancelled when this
	// process is no longer responsible for the shard.
	Context() context.Context
	// Fully-qualified name of this shard, which composes the consumer application
	// prefix with this shard's ID. Specifically, this is the Etcd item key of the
	// ShardSpec, which ensures a Shard FQN can only conflict if another consumer
	// deployment, backed by a different Etcd cluster, reuses the same application
	// root and ShardID.
	FQN() string
	// Spec of the shard.
	Spec() *pc.ShardSpec
	// Assignment of the shard to this process.
	Assignment() keyspace.KeyValue
	// JournalClient to be used for raw journal []byte appends made on behalf
	// of this Shard. Consistent use of this client enables Gazette to ensure
	// that all writes issued within a consumer transaction have completed prior
	// to that transaction being committed. Put differently, if a consumed message
	// triggers a raw append to a journal, Gazette can guarantee that append will
	// occur at-least-once no matter how this Shard or brokers may fail.
	JournalClient() client.AsyncJournalClient
	// Progress of the Shard as-of its most recent completed transaction.
	// |readThrough| is offsets of source journals which have been read
	// through. |publishAt| is journals and offsets this Shard has published
	// through, including acknowledgements. If a read message A results in this
	// Shard publishing messages B, and A falls within |readThrough|, then all
	// messages B (& their acknowledgements) fall within |publishAt|.
	//
	// While |readThrough| is comprehensive and persists across Shard faults,
	// note that |publishAt| is *advisory* and not necessarily complete: it
	// includes only journals written to since this Shard was assigned to
	// this process.
	Progress() (readThrough, publishAt pb.Offsets)
}

// Store is a stateful storage backend. Often Stores are implemented as embedded
// databases which record their file operations into a provided Recorder.
// Stores which instead utilize an external transactional system (eg, an RDBMS)
// are also supported. Application implementations control the selection,
// initialization, and usage of an appropriate Store backend for their use case.
type Store interface {
	// StartCommit starts a background, atomic "commit" to the Store of state
	// updates from this transaction along with the Checkpoint. If Store uses an
	// external transactional system, then StartCommit() must fail if another
	// process has invoked RestoreCheckpoint() after *this* Store instance did
	// so. Put differently, Store cannot commit a Checkpoint that another Store
	// may never see (because it continues an earlier Checkpoint that it restored).
	//
	// In general terms, this means StartCommit() must verify a "write fence"
	// previously installed by RestoreCheckpoint() as part of its transaction.
	// Embedded Stores which use recovery logs can rely on the write fence of
	// the log itself.
	//
	// StartCommit may immediately begin a transaction with the external system
	// (if one hasn't already been started from ConsumeMessage()), but cannot
	// allow it to commit until all |waitFor| OpFutures have completed
	// successfully. A failure of one of these OpFutures must also fail this
	// commit.
	//
	// StartCommit() will never be called if the OpFuture returned by a previous
	// StartCommit() hasn't yet completed. However, ConsumeMessage() *will* be
	// called while a previous StartCommit() continues to run. Stores should
	// account for this, typically by starting a new transaction which runs
	// alongside the old.
	StartCommit(_ Shard, _ pc.Checkpoint, waitFor OpFutures) OpFuture
	// RestoreCheckpoint recovers the most recent Checkpoint previously committed
	// to the Store. It is called just once, at Shard start-up. If an external
	// system is used, it should install a transactional "write fence" to ensure
	// that an older Store instance of another process cannot successfully
	// StartCommit() after this RestoreCheckpoint() returns.
	RestoreCheckpoint(Shard) (pc.Checkpoint, error)
	// Destroy releases all resources associated with the Store (eg, local files).
	// It is guaranteed that the Store is no longer being used or referenced at
	// the onset of this call.
	Destroy()
}

// OpFuture represents an operation which is executing in the background.
// Aliased for brevity from `client` package.
type OpFuture = client.OpFuture
type OpFutures = client.OpFutures

// Application is the interface provided by user applications running as Gazette
// consumers. Only unrecoverable errors should be returned by Application.
// A returned error will abort processing of an assigned Shard, and will update
// the assignment's ReplicaStatus to FAILED.
//
// Gazette consumers process messages within pipelined transactions. A
// transaction begins upon the first call to ConsumeMessage(), which is invoked
// for each read-committed message of a source journal. In the course of the
// transaction many more messages may be passed to ConsumeMessage(). When
// consuming a message the Application is free to:
//
// 1) Begin or continue a transaction with its Store.
// 2) Publish exactly-once Messages to other journals via the provided Publisher.
// 3) Append raw at-least-once []bytes via the shard's JournalClient().
// 4) To keep in-memory-only aggregates such as counts.
//
// Messages published via PublishUncommitted will be visible to read-committed
// readers once the consumer transaction completes. Read-uncommitted readers will see
// them while the transaction continues to run. Similarly writes issued directly
// through the shard JournalClient() are also readable while the transaction runs.
//
// Eventually, either because of a read stall or the maximum duration is reached,
// the transaction is closed to new messages and FinalizeTxn() is called. At this
// point the Application must publish any remaining messages or begin related
// journal writes, and must flush any in-memory caches or aggregates into its
// Store transaction (the Application does _not_ need to wait for journal writes
// to complete).
//
// StartCommit() of the Store is then called with a Checkpoint. For correct
// exactly-once processing semantics, the Checkpoint must be written in a
// single transaction alongside all other Store mutations made within the
// scope of this consumer transaction:
//
// * Eg for RocksDB, this means all mutations and the Checkpoint must be written
//   within a single "write batch".
// * For a SQL store, verification of the write fence, INSERTS, UPDATES, and the
//   Checkpoint itself are written within a single BEGIN/COMMIT transaction.
//
// Note that other, non-transactional Store mutations are permitted, but will
// have a weaker at-least-once processing guarantee with respect to Store state.
// This can make sense for applications filling caches in BigTable, Memcache, or
// other systems support transactions only over a single key (ie, check-and-set).
// In this case the Store should apply all mutations, followed by a fenced CAS
// Checkpoint update. So long as the Checkpoint itself is fenced properly,
// messages will continue to have exactly-once semantics (though it gets murky
// if published messages are conditioned on Store states that update at-least-once).
//
// Once the commit completes, acknowledgements of messages published during the
// transaction are written to applicable journals, which informs downstream
// readers that those messages have committed and may now be consumed.
//
// Transactions are fully pipelined: once StartCommit() has returned, the next
// consumer transaction immediately begins processing even though the prior
// transaction continues to commit in the background (and could even fail). In
// fact, at this point there's no guarantee that any journal writes of the
// previous transaction have even _started_, including those to the recovery log.
//
// To make this work safely and correctly, transactions use barriers to ensure
// that background operations are started and complete in the correct order:
// for example, that the prior transaction doesn't commit until all its writes
// to other journals have also completed, and that writes of message
// acknowledgements don't start until mutations & the checkpoint have committed
// to the Store.
//
// While those operations complete in the background, the next transaction will
// consume new messages concurrently. Its one constraint is that it may not
// itself start to commit until its predecessor transaction has fully completed.
// This means that transactions will almost always exhibit some degree of
// batching, which will depend on the rate of incoming messages, the latency to
// Gazette, and the latency and speed of a utilized external store. If the prior
// commit takes so long that the successor transaction reaches its maximum
// duration, then that successor will stall without processing further messages
// until its predecessor commits. This is the _only_ case under which a consumer
// can be blocked from processing ready messages.
type Application interface {
	// NewStore constructs a Store for the Shard around the initialize file-
	// system *Recorder. If the ShardSpec does not have a configured recovery
	// log, then *Recorder will be nil.
	NewStore(Shard, *recoverylog.Recorder) (Store, error)
	// NewMessage returns a zero-valued Message of an appropriate representation
	// for the JournalSpec.
	NewMessage(*pb.JournalSpec) (message.Message, error)
	// ConsumeMessage consumes a read-committed message within the scope of a
	// transaction. It should use the provided Publisher to PublishUncommitted
	// messages to other journals. Doing so enables Gazette to properly sequence
	// messages and ensure they are either acknowledged or rolled-back atomically
	// with this consumer transaction.
	ConsumeMessage(Shard, Store, message.Envelope, *message.Publisher) error
	// FinalizeTxn indicates a consumer transaction is ending, and that the
	// Application must flush any in-memory transaction state or caches, and
	// issued any relevant journal writes. At completion all messages must have
	// been published to the provided Publisher, any raw []byte content must
	// have been written to the shard JournalClient(), and all state must be
	// captured by the Store.
	FinalizeTxn(Shard, Store, *message.Publisher) error
}

// BeginFinisher is an optional interface of Application which is informed
// when consumer transactions begin or finish.
type BeginFinisher interface {
	// BeginTxn is called to notify the Application that a transaction is beginning
	// (and a call to ConsumeMessage will be immediately forthcoming), allowing
	// the Application to perform any preparatory work. For consumers doing
	// extensive aggregation, it may be beneficial to focus available compute
	// resource on a small number of transactions while completely stalling
	// others: this can be accomplished by blocking in BeginTxn until a semaphore
	// is acquired. A call to BeginTx is always paired with a call to FinishTxn.
	BeginTxn(Shard, Store) error
	// FinishedTxn is notified that a previously begun transaction has started to
	// commit, or has errored. It allows the Application to perform related cleanup
	// (eg, releasing a previously acquired semaphore). Note transactions are
	// pipelined, and commit operations of this transaction may still be ongoing.
	// FinishedTxn can await the provided OpFuture for its final status.
	FinishedTxn(Shard, Store, OpFuture)
}
