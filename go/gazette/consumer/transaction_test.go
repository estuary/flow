package consumer

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

func TestTxnPriorSyncsThenMinDurElapses(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	require.False(t, prior.prepareDoneAt.IsZero())
	require.Equal(t, txn.prevPrepareDoneAt, prior.prepareDoneAt)
	require.True(t, prior.committedAt.IsZero())

	// |prior| commits.
	timer.timepoint = faketime(500 * time.Millisecond)
	priorCommit.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, prior.committedAt.IsZero())
	require.True(t, prior.ackedAt.IsZero())

	// |prior| ACKs.
	var signalCh = shard.progress.signalCh
	timer.timepoint = faketime(1 * time.Second)
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, prior.ackedAt.IsZero())
	<-signalCh // Progress updated.

	// Expect time delta flows into shard clock update, below.
	tf.service.PublishClockDelta = time.Hour

	// Initial message opens the txn.
	timer.timepoint = faketime(2 * time.Second)
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, minDur, timer.reset)                                         // Was Reset to |minDur|.
	require.Equal(t, message.NewClock(txn.beganAt.Add(time.Hour))+160, shard.clock) // Shard clock was updated.

	// Expect it continues to block.
	require.True(t, txnBlocks(shard, &txn))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	require.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(2*time.Second + minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Consume additional ready message.
	require.False(t, txnBlocks(shard, &txn))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "3"})
	msgCh <- <-msgCh // Ensure message is buffered.
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |msgCh| stalls, and the transaction begins to commit.
	timer.timepoint = faketime(2*time.Second + minDur + 1)
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	require.True(t, timer.stopped)
	require.Equal(t, time.Duration(-1), txn.minDur)
	require.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	require.NotNil(t, txn.readCh)        // Did not reach maxDur.
	require.Equal(t, 3, txn.consumedCount)
	require.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "3"})
	require.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	require.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	require.Equal(t, prior.prepareDoneAt, faketime(0))
	require.Equal(t, prior.committedAt, faketime(500*time.Millisecond))
	require.Equal(t, prior.ackedAt, faketime(time.Second))
	require.Equal(t, txn.prevPrepareDoneAt, faketime(0))
	require.Equal(t, txn.beganAt, faketime(2*time.Second))
	require.Equal(t, txn.stalledAt, faketime(2*time.Second+minDur+1))
	require.Equal(t, txn.prepareBeganAt, faketime(2*time.Second+minDur+1))
	require.Equal(t, txn.prepareDoneAt, faketime(2*time.Second+minDur+1))
}

func TestTxnMinDurElapsesThenPriorSyncs(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// Initial message opens the txn.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Expect it continues to block.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	require.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// |prior| commits.
	timer.timepoint = faketime(minDur + 1)
	priorCommit.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, prior.committedAt.IsZero())
	require.True(t, prior.ackedAt.IsZero())

	require.True(t, txnBlocks(shard, &txn)) // Still blocking.

	// |prior| ACKs.
	timer.timepoint = faketime(minDur + 2)
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, prior.ackedAt.IsZero())

	// Consume additional ready message.
	require.False(t, txnBlocks(shard, &txn))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "3"})
	msgCh <- <-msgCh // Ensure message is buffered.
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |msgCh| stalls, and the transaction begins to commit.
	timer.timepoint = faketime(minDur + 3)
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	require.True(t, timer.stopped)
	require.Equal(t, time.Duration(-1), txn.minDur)
	require.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	require.NotNil(t, txn.readCh)        // Did not reach maxDur.
	require.Equal(t, 3, txn.consumedCount)
	require.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "3"})
	require.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	require.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	require.Equal(t, prior.prepareDoneAt, faketime(0))
	require.Equal(t, prior.committedAt, faketime(minDur+1))
	require.Equal(t, prior.ackedAt, faketime(minDur+2))
	require.Equal(t, txn.prevPrepareDoneAt, faketime(0))
	require.Equal(t, txn.beganAt, faketime(0))
	require.Equal(t, txn.stalledAt, faketime(minDur+3))
	require.Equal(t, txn.prepareBeganAt, faketime(minDur+3))
	require.Equal(t, txn.prepareDoneAt, faketime(minDur+3))
}

func TestTxnMaxDurElapsesThenPriorSyncs(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// Initial message opens the txn.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Expect it continues to block.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	require.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |maxDur| has elapsed.
	timer.timepoint = faketime(maxDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// Additional messages will not be consumed.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "not read"})
	msgCh <- <-msgCh // Ensure message is buffered.

	// |prior| commits.
	timer.timepoint = faketime(maxDur + 1)
	priorCommit.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |prior| ACKs.
	timer.timepoint = faketime(maxDur + 2)
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |msgCh| stalls, and the transaction begins to commit.
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	require.False(t, timer.stopped) // Was already stopped.
	require.Equal(t, time.Duration(-1), txn.minDur)
	require.Equal(t, time.Duration(-1), txn.maxDur)
	require.Nil(t, txn.readCh) // Reached maxDur.
	require.Equal(t, 2, txn.consumedCount)
	require.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "2"})
	require.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	require.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	require.Equal(t, prior.prepareDoneAt, faketime(0))
	require.Equal(t, prior.committedAt, faketime(maxDur+1))
	require.Equal(t, prior.ackedAt, faketime(maxDur+2))
	require.Equal(t, txn.prevPrepareDoneAt, faketime(0))
	require.Equal(t, txn.beganAt, faketime(0))
	require.Equal(t, txn.stalledAt, faketime(maxDur))
	require.Equal(t, txn.prepareBeganAt, faketime(maxDur+2))
	require.Equal(t, txn.prepareDoneAt, faketime(maxDur+2))
}

func TestTxnDoesntStartUntilFirstACK(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp    = playAndComplete(t, shard)
		msgCh = make(chan EnvelopeOrError, 1)
		timer = newTestTimer()
		prior = transaction{
			commitBarrier: client.FinishedOperation(nil),
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// |prior| commits and ACKs.
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, prior.committedAt.IsZero())
	require.False(t, prior.ackedAt.IsZero())

	// Publish some pending messages.
	for _, key := range []string{"A", "B", "C"} {
		_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: key, Value: "v"})
		require.False(t, mustTxnStep(t, shard, &txn, &prior))
	}

	// Expect transaction hasn't started yet.
	require.Equal(t, 0, txn.consumedCount) // None read yet.
	require.Zero(t, timer.reset)

	tf.writeTxnPubACKs()

	// Transaction starts, and all ACKd messages are read.
	timer.timepoint = faketime(time.Second)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.
	require.Equal(t, 4, txn.consumedCount)

	require.True(t, txnBlocks(shard, &txn))

	// |minDur| has elapsed.
	timer.timepoint = faketime(time.Second + minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.
	require.False(t, txnBlocks(shard, &txn))     // May now commit.

	// Another ready, pending message is read.
	_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "D", Value: "v"})
	msgCh <- <-msgCh // Ensure message is buffered.
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.True(t, txnBlocks(shard, &txn)) // Wait for ACK.

	// ACK pending message, and read.
	timer.timepoint = faketime(time.Second + minDur + 1)
	tf.writeTxnPubACKs()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, txnBlocks(shard, &txn)) // May commit again.

	// |msgCh| stalls, and the transaction begins to commit.
	timer.timepoint = faketime(time.Second + minDur + 2)
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	require.True(t, timer.stopped)
	require.Equal(t, time.Duration(-1), txn.minDur)
	require.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	require.NotNil(t, txn.readCh)        // Did not reach maxDur.
	require.Equal(t, 6, txn.consumedCount)

	verifyStoreAndEchoOut(t, shard, map[string]string{"A": "v", "B": "v", "C": "v", "D": "v"})
	require.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)
	require.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])

	require.Equal(t, prior.prepareDoneAt, faketime(0))
	require.Equal(t, prior.committedAt, faketime(0))
	require.Equal(t, prior.ackedAt, faketime(0))
	require.Equal(t, txn.prevPrepareDoneAt, faketime(0))
	require.Equal(t, txn.beganAt, faketime(time.Second))
	require.Equal(t, txn.stalledAt, faketime(time.Second+minDur+2))
	require.Equal(t, txn.prepareBeganAt, faketime(time.Second+minDur+2))
	require.Equal(t, txn.prepareDoneAt, faketime(time.Second+minDur+2))
}

func TestTxnReadChannelDrain(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		altMsgCh    = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, altMsgCh, timer.txnTimer)

	// Initial message opens the txn.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	altMsgCh <- <-msgCh
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.

	// Close of channel is read. Expect it continues to block.
	close(altMsgCh)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |prior| commits and ACKs. Still blocking.
	priorCommit.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset)

	// Next poll of the transaction stalls, and it begins to commit.
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	require.True(t, timer.stopped)
	require.Equal(t, time.Duration(-1), txn.minDur)
	require.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	require.Nil(t, txn.readCh)           // Closed on read EOF.
	require.Equal(t, 1, txn.consumedCount)
	require.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "1"})
	require.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	require.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	require.Equal(t, prior.prepareDoneAt, faketime(0))
	require.Equal(t, prior.committedAt, faketime(0))
	require.Equal(t, prior.ackedAt, faketime(0))
	require.Equal(t, txn.prevPrepareDoneAt, faketime(0))
	require.Equal(t, txn.beganAt, faketime(0))
	require.Equal(t, txn.stalledAt, faketime(minDur))
	require.Equal(t, txn.prepareBeganAt, faketime(minDur))
	require.Equal(t, txn.prepareDoneAt, faketime(minDur))
}

func TestIdleTxnUpdatesProgressOnDuplicateACKs(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
			checkpoint: pc.Checkpoint{
				Sources: make(map[pb.Journal]pc.Checkpoint_Source),
			},
		}
		txn = transaction{}
	)

	// Install a fixture such that all messages of this publisher are duplicates.
	shard.sequencer = message.NewSequencer(
		pc.FlattenReadThrough(cp),
		[]message.ProducerState{
			{
				JournalProducer: message.JournalProducer{
					Journal:  sourceA.Name,
					Producer: tf.pub.ProducerID(),
				},
				LastAck: math.MaxUint64 - 1,
				Begin:   -1,
			},
		},
		12,
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// A duplicate message is received, which does not begin a transaction.
	aa, _ := tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	require.NoError(t, aa.Err())
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, 0, txn.consumedCount) // None read yet.

	// |prior| checkpoint was updated with duplicate offset extent.
	require.Equal(t, pc.Checkpoint_Source{ReadThrough: aa.Response().Commit.End},
		prior.checkpoint.Sources[sourceA.Name])

	// |prior| commits and ACKs.
	var signalCh = shard.progress.signalCh
	priorCommit.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	<-signalCh // Progress updated.

	require.Equal(t, shard.progress.readThrough[sourceA.Name], aa.Response().Commit.End)

	// Another duplicate message is received. Again it doesn't begin a transaction.
	aa, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	require.NoError(t, aa.Err())

	// This time progress is updated directly, since there's no running prior transaction.
	signalCh = shard.progress.signalCh
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, 0, txn.consumedCount) // None read yet.
	<-signalCh
	require.Equal(t, shard.progress.readThrough[sourceA.Name], aa.Response().Commit.End)
}

func TestTxnMaxDurWithinDequeueSequence(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		_              = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// Write a committed message sequence which opens the stream.
	_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "one", Value: "1"})
	_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "two", Value: "2"})
	var aa = tf.writeTxnPubACKs()[0]
	require.NoError(t, aa.Err())

	// Run until first message is processed. Second and ACK remain to dequeue.
	for shard.sequencer.Dequeued == nil {
		var _, err = txnStep(shard, &txn, &prior)
		require.NoError(t, err)
	}
	var _, err = txnStep(shard, &txn, &prior) // Consume first message.
	require.NoError(t, err)

	// Signal that |maxDur| has elapsed. No further messages are read.
	timer.timepoint = faketime(maxDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Nil(t, txn.readCh) // Nil'd to stop further message processing.

	// |prior| commits and ACKs.
	priorCommit.Resolve(nil)
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// Poll once more to commit.
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	verifyStoreAndEchoOut(t, shard, map[string]string{"one": "1"})
	require.NotNil(t, shard.sequencer.Dequeued)
	require.Greater(t, aa.Response().Commit.End,
		txn.checkpoint.Sources[sourceA.Name].ReadThrough)

	// Next transaction starts, and processes remaining messages.
	prior, txn = txn, prior
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |minDur| elapses.
	timer.timepoint = faketime(maxDur + minDur)
	timer.signal()
	require.NoError(t, txnRun(shard, &txn, &prior)) // Run remainder.

	// Only *now* have we processed through the original sequence.
	verifyStoreAndEchoOut(t, shard, map[string]string{"one": "1", "two": "2"})
	require.Equal(t, aa.Response().Commit.End,
		txn.checkpoint.Sources[sourceA.Name].ReadThrough)
}

func TestAppDeferWithinDequeueSequence(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan EnvelopeOrError, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		timer       = newTestTimer()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
			prepareDoneAt: timer.timepoint,
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{}
		_              = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// Write a committed message sequence which opens the stream.
	_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "one", Value: "1"})
	_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "two", Value: "2"})
	var aa = tf.writeTxnPubACKs()[0]
	require.NoError(t, aa.Err())

	// Run through until we've processed the first message.
	for shard.sequencer.Dequeued == nil {
		var _, err = txnStep(shard, &txn, &prior)
		require.NoError(t, err)
	}
	var _, err = txnStep(shard, &txn, &prior) // Consume first message.
	require.NoError(t, err)

	// Application elects to defer further processing.
	tf.app.consumeErr = ErrDeferToNextTransaction
	require.False(t, mustTxnStep(t, shard, &txn, &prior)) // Attempt to consume second message.
	require.Nil(t, txn.readCh)                            // Nil'd to stop further message processing.

	// |prior| commits and ACKs.
	priorCommit.Resolve(nil)
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(maxDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// Poll once more to commit.
	require.True(t, mustTxnStep(t, shard, &txn, &prior))

	verifyStoreAndEchoOut(t, shard, map[string]string{"one": "1"})
	require.NotNil(t, shard.sequencer.Dequeued)
	require.Greater(t, aa.Response().Commit.End,
		txn.checkpoint.Sources[sourceA.Name].ReadThrough)

	// Next transaction starts, and processes remaining messages.
	prior, txn = txn, prior
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)
	tf.app.consumeErr = nil // Don't error again.
	require.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |minDur| elapses.
	timer.timepoint = faketime(maxDur + minDur)
	timer.signal()
	require.NoError(t, txnRun(shard, &txn, &prior)) // Run remainder.

	// Only *now* have we processed through the original sequence.
	verifyStoreAndEchoOut(t, shard, map[string]string{"one": "1", "two": "2"})
	require.Equal(t, aa.Response().Commit.End,
		txn.checkpoint.Sources[sourceA.Name].ReadThrough)
}

func TestRunTxnsACKsRecoveredCheckpoint(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var cp = playAndComplete(t, shard)
	cp.AckIntents = map[pb.Journal][]byte{
		echoOut.Name: []byte(`{"Key": "recovered fixture"}` + "\n"),
		// Recovered ACK intents may included journals which do not exist.
		"does/not/exist": []byte(`{"Key": "discarded fixture"}` + "\n"),
	}

	// Use a read channel fixture which immediately closes.
	var readCh = make(chan EnvelopeOrError)
	close(readCh)
	require.NoError(t, runTransactions(shard, cp, readCh, nil))

	// Expect the ACK intent fixture is written to |echoOut|.
	var rr = client.NewRetryReader(context.Background(), tf.ajc, pb.ReadRequest{
		Journal: echoOut.Name,
		Block:   true,
	})
	var it = message.NewReadUncommittedIter(rr, new(testApplication).NewMessage)

	var env, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, &testMessage{Key: "recovered fixture"}, env.Message)
}

func TestRunTxnsUpdatesRecordedHints(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp      = playAndComplete(t, shard)
		msgCh   = make(chan EnvelopeOrError, 1)
		hintsCh = make(chan time.Time, 1)
	)
	startReadingMessages(shard, cp, msgCh)

	go func() {
		require.True(t, errors.Is(runTransactions(shard, cp, msgCh, hintsCh), context.Canceled))
	}()
	// Precondition: recorded hints are not set.
	require.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintPrimaryKey()).Kvs, 0)

	hintsCh <- time.Time{}

	// runTransactions does a non-blocking select of |hintsCh|, so run two txns
	// to ensure that |hintsCh| is selected and the operation has completed.
	runTransaction(tf, shard, map[string]string{"one": ""})
	runTransaction(tf, shard, map[string]string{"two": ""})

	require.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintPrimaryKey()).Kvs, 1)
}

func TestRunTxnsAppErrorCases(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp    = playAndComplete(t, shard)
		msgCh = make(chan EnvelopeOrError, 1)
	)
	startReadingMessages(shard, cp, msgCh)

	var cases = []struct {
		fn           func()
		expectErr    string
		expectFinish bool
	}{
		{
			fn:           func() { tf.app.finalizeErr = errors.New("finalize error") },
			expectErr:    "txnStartCommit: app.FinalizeTxn: finalize error",
			expectFinish: true,
		},
		{
			fn:           func() { tf.app.consumeErr = errors.New("consume error") },
			expectErr:    "app.ConsumeMessage: consume error",
			expectFinish: true,
		},
		{
			fn:           func() { tf.app.consumeErr = ErrDeferToNextTransaction },
			expectErr:    "consumer transaction is empty, but application deferred the first message",
			expectFinish: true,
		},
		{
			fn:           func() { tf.app.beginErr = errors.New("begin error") },
			expectErr:    "app.BeginTxn: begin error",
			expectFinish: false,
		},
	}
	for _, tc := range cases {
		tc.fn()

		_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "val"})
		require.EqualError(t, runTransactions(shard, cp, msgCh, nil), tc.expectErr)

		if tc.expectFinish {
			<-(<-tf.app.finishedCh).Done()
		}
	}
}

func mustTxnStep(t require.TestingT, s *shard, txn, prior *transaction) bool {
	var done, err = txnStep(s, txn, prior)
	require.NoError(t, err)

	for txn.readCh != nil && s.sequencer.Dequeued != nil {
		done, err = txnStep(s, txn, prior)
		require.NoError(t, err)
	}
	return done
}
