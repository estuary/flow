// Package consumer is a library for distributed, stateful consumption of Gazette journals.

//go:generate protoc -I . -I ../../../vendor -I ../../../../../.. --gogo_out=plugins=grpc:. consumer.proto
package consumer

import (
	"context"

	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
)

// Shard is the processing context of a ShardSpec which is assigned to the
// local consumer process.
type Shard interface {
	// Context of this shard. Notably, the Context will be cancelled when this
	// process is no longer responsible for the shard.
	Context() context.Context
	// Spec of the shard.
	Spec() *ShardSpec
	// Assignment of the shard to this process.
	Assignment() keyspace.KeyValue
	// JournalClient for broker operations performed in the course of processing
	// this Shard. Applications should use this AsyncJournalClient to allow
	// consumer transactions to track and appropriately sync on written journals.
	JournalClient() client.AsyncJournalClient
}

// Store is a stateful storage backend which is minimally able to record its file
// operations into a provided recoverylog.Recorder, and is able to represent a
// collection of Journal offsets. Application implementations control the selection,
// initialization, and usage of an appropriate Store backend for their use case.
type Store interface {
	// Recorder which this Store wraps, into which all Store state is recorded.
	Recorder() *recoverylog.Recorder
	// FetchJournalOffsets returns the collection of Journals and corresponding
	// offsets represented within the Store.
	FetchJournalOffsets() (map[pb.Journal]int64, error)
	// Flush |offsets| and any in-memory Store state to disk.
	Flush(offsets map[pb.Journal]int64) error
	// Destroy releases all resources and removes all local files associated
	// with the Store. It is guaranteed that the Store is no longer being used or
	// referenced at the onset of this call.
	Destroy()
}

// Application is the interface provided by domain applications
// running as Gazette consumers. Only unrecoverable errors should be
// returned by Application. A returned error will abort processing of an
// assigned Shard, and will update the assignment's ReplicaStatus to FAILED.
//
// Gazette consumers process messages within pipelined transactions. A
// transaction begins upon receipt of a new message from a read journal
// at its present read offset. The message is consumed, and in the course
// of the transaction many more messages may be read & consumed. When
// consuming a message, the Application is free to consult the Store, to
// publish other messages to other journals via the Shard AsyncJournalClient,
// or to simply keep in-memory-only aggregates such as counts.
//
// Eventually, either because of a read stall or maximum duration, the
// transaction will be finalized, at which point the Application must at
// least _start_ any related journal writes, and must reflect any in-memory
// caches or aggregates back into the Store.
//
// The offsets which have been read through are also flushed to the Store
// at this time, and to disk, often as an atomic update mixed with Application
// state updates (when atomic writes are used, the framework provides an
// exactly-once guarantee over message state updates).
//
// This final flush to the Store (and its recoverylog) is made dependent
// upon all other writes issued to the AsyncJournalClient, ensuring that read
// offsets are persisted only after related writes have completed, and thereby
// providing an at-least once guarantee for journal writes driven by the
// transaction.
//
// Transactions are pipelined, which means that while asynchronous writes
// from the finalized transaction run to completion, another consumer
// transaction may begin processing concurrently. That new transaction is
// constrained only in that it may not itself finalize until all writes of its
// predecessor have fully committed to the brokers. If that takes so long that
// the successor transaction reaches its maximum duration, then that successor
// will stall without processing further messages until all predecessor writes
// commit.
type Application interface {
	// NewStore constructs a Store for |shard| around the recovered local directory
	// |dir| and initialized Recorder |rec|.
	NewStore(shard Shard, dir string, rec *recoverylog.Recorder) (Store, error)
	// NewMessage returns a zero-valued Message of an appropriate representation
	// for the JournalSpec.
	NewMessage(*pb.JournalSpec) (message.Message, error)

	// ConsumeMessage consumes a message within the scope of a transaction.
	ConsumeMessage(Shard, Store, message.Envelope) error
	// FinalizeTxn indicates a consumer transaction is ending, and that the
	// Application must flush any in-memory transaction state or caches, and
	// issued any relevant journal writes. At completion all writes must have
	// been published to the Shard AsyncJournalClient, and all state must be captured
	// by the Store.
	FinalizeTxn(shard Shard, store Store) error
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
	// FinishTxn is notified that a begun transaction has completed, and should
	// perform any related cleanup (eg, releasing a previously acquired
	// semaphore). Note that, because transactions are pipelined, underlying
	// journal writes of the transaction may still be ongoing
	FinishTxn(Shard, Store)
}
