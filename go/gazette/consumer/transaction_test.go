package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

func TestTxnPriorSyncsThenMinDurElapses(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var timer, restore = newTestTimer()
	defer restore()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan readMessage, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{readThrough: make(pb.Offsets)}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// |prior| commits.
	priorCommit.Resolve(nil)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.False(t, prior.committedAt.IsZero())
	assert.True(t, prior.ackedAt.IsZero())

	// |prior| ACKs.
	var signalCh = shard.progress.signalCh
	timer.timepoint = faketime(1 * time.Second)
	priorAck.Resolve(nil)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.False(t, prior.ackedAt.IsZero())
	<-signalCh // Progress updated.

	// Initial message opens the txn.
	timer.timepoint = faketime(2 * time.Second)
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, minDur, timer.reset)                          // Was Reset to |minDur|.
	assert.Equal(t, message.NewClock(txn.beganAt)+1, shard.clock) // Shard clock was updated.

	// Expect it continues to block.
	assert.True(t, txnBlocks(shard, &txn, &prior))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	assert.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(2*time.Second + minDur)
	timer.signal()
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Consume additional ready message.
	assert.False(t, txnBlocks(shard, &txn, &prior))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "3"})
	msgCh <- <-msgCh // Ensure message is buffered.
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |msgCh| stalls, and the transaction begins to commit.
	timer.timepoint = faketime(2*time.Second + minDur + 1)
	assert.True(t, mustTxnStep(t, shard, &txn, &prior))

	assert.True(t, timer.stopped)
	assert.Equal(t, time.Duration(-1), txn.minDur)
	assert.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	assert.NotNil(t, txn.readCh)        // Did not reach maxDur.
	assert.Equal(t, 3, txn.consumedCount)
	assert.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "3"})
	assert.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	assert.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	assert.Equal(t, prior.committedAt, faketime(0))
	assert.Equal(t, prior.ackedAt, faketime(time.Second))
	assert.Equal(t, txn.beganAt, faketime(2*time.Second))
	assert.Equal(t, txn.stalledAt, faketime(2*time.Second+minDur+1))
	assert.Equal(t, txn.prepareBeganAt, faketime(2*time.Second+minDur+1))
	assert.Equal(t, txn.prepareDoneAt, faketime(2*time.Second+minDur+1))
}

func TestTxnMinDurElapsesThenPriorSyncs(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var timer, restore = newTestTimer()
	defer restore()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan readMessage, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{readThrough: make(pb.Offsets)}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// Initial message opens the txn.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(minDur)
	timer.signal()
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Expect it continues to block.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	assert.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// |prior| commits.
	timer.timepoint = faketime(minDur + 1)
	priorCommit.Resolve(nil)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.False(t, prior.committedAt.IsZero())
	assert.True(t, prior.ackedAt.IsZero())

	assert.True(t, txnBlocks(shard, &txn, &prior)) // Still blocking.

	// |prior| ACKs.
	timer.timepoint = faketime(minDur + 2)
	priorAck.Resolve(nil)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.False(t, prior.ackedAt.IsZero())

	// Consume additional ready message.
	assert.False(t, txnBlocks(shard, &txn, &prior))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "3"})
	msgCh <- <-msgCh // Ensure message is buffered.
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |msgCh| stalls, and the transaction begins to commit.
	timer.timepoint = faketime(minDur + 3)
	assert.True(t, mustTxnStep(t, shard, &txn, &prior))

	assert.True(t, timer.stopped)
	assert.Equal(t, time.Duration(-1), txn.minDur)
	assert.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	assert.NotNil(t, txn.readCh)        // Did not reach maxDur.
	assert.Equal(t, 3, txn.consumedCount)
	assert.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "3"})
	assert.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	assert.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	assert.Equal(t, prior.committedAt, faketime(minDur+1))
	assert.Equal(t, prior.ackedAt, faketime(minDur+2))
	assert.Equal(t, txn.beganAt, faketime(0))
	assert.Equal(t, txn.stalledAt, faketime(minDur+3))
	assert.Equal(t, txn.prepareBeganAt, faketime(minDur+3))
	assert.Equal(t, txn.prepareDoneAt, faketime(minDur+3))
}

func TestTxnMaxDurElapsesThenPriorSyncs(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var timer, restore = newTestTimer()
	defer restore()

	var (
		cp          = playAndComplete(t, shard)
		msgCh       = make(chan readMessage, 1)
		priorCommit = client.NewAsyncOperation()
		priorAck    = client.NewAsyncOperation()
		prior       = transaction{
			commitBarrier: priorCommit,
			acks:          OpFutures{priorAck: {}},
		}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{readThrough: make(pb.Offsets)}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// Initial message opens the txn.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "1"})
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(minDur)
	timer.signal()
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Expect it continues to block.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	assert.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |maxDur| has elapsed.
	timer.timepoint = faketime(maxDur)
	timer.signal()
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))

	// Additional messages will not be consumed.
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "not read"})
	msgCh <- <-msgCh // Ensure message is buffered.

	// |prior| commits.
	timer.timepoint = faketime(maxDur + 1)
	priorCommit.Resolve(nil)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |prior| ACKs.
	timer.timepoint = faketime(maxDur + 2)
	priorAck.Resolve(nil)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))

	// |msgCh| stalls, and the transaction begins to commit.
	assert.True(t, mustTxnStep(t, shard, &txn, &prior))

	assert.False(t, timer.stopped) // Was already stopped.
	assert.Equal(t, time.Duration(-1), txn.minDur)
	assert.Equal(t, time.Duration(-1), txn.maxDur)
	assert.Nil(t, txn.readCh) // Reached maxDur.
	assert.Equal(t, 2, txn.consumedCount)
	assert.Len(t, txn.acks, 1)

	verifyStoreAndEchoOut(t, shard, map[string]string{"key": "2"})
	assert.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])
	assert.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)

	assert.Equal(t, prior.committedAt, faketime(maxDur+1))
	assert.Equal(t, prior.ackedAt, faketime(maxDur+2))
	assert.Equal(t, txn.beganAt, faketime(0))
	assert.Equal(t, txn.stalledAt, faketime(maxDur))
	assert.Equal(t, txn.prepareBeganAt, faketime(maxDur+2))
	assert.Equal(t, txn.prepareDoneAt, faketime(maxDur+2))
}

func TestTxnDoesntStartUntilFirstACK(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var timer, restore = newTestTimer()
	defer restore()

	var (
		cp             = playAndComplete(t, shard)
		msgCh          = make(chan readMessage, 1)
		prior          = transaction{commitBarrier: client.FinishedOperation(nil)}
		minDur, maxDur = shard.Spec().MinTxnDuration, shard.Spec().MaxTxnDuration
		txn            = transaction{readThrough: make(pb.Offsets)}
		store          = shard.store.(*JSONFileStore)
	)
	startReadingMessages(shard, cp, msgCh)
	txnInit(shard, &txn, &prior, msgCh, timer.txnTimer)

	// |prior| commits and ACKs.
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.False(t, prior.committedAt.IsZero())
	assert.False(t, prior.ackedAt.IsZero())

	// Publish some pending messages.
	for _, key := range []string{"A", "B", "C"} {
		_ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: key, Value: "v"})
		assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	}

	// Expect transaction hasn't started yet.
	assert.Equal(t, 0, txn.consumedCount) // None read yet.
	assert.Zero(t, timer.reset)

	tf.writeTxnPubACKs()

	// Transaction starts, and all ACKd messages are read.
	timer.timepoint = faketime(time.Second)
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, minDur, timer.reset) // Was Reset to |minDur|.
	assert.Equal(t, 4, txn.consumedCount)

	assert.True(t, txnBlocks(shard, &txn, &prior))

	// |minDur| has elapsed.
	timer.timepoint = faketime(time.Second + minDur)
	timer.signal()
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.Equal(t, maxDur-minDur, timer.reset)     // Was Reset to |maxDur| remainder.
	assert.False(t, txnBlocks(shard, &txn, &prior)) // May now commit.

	// Another ready, pending message is read.
	_ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "D", Value: "v"})
	msgCh <- <-msgCh // Ensure message is buffered.
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.True(t, txnBlocks(shard, &txn, &prior)) // Wait for ACK.

	// ACK pending message, and read.
	timer.timepoint = faketime(time.Second + minDur + 1)
	tf.writeTxnPubACKs()
	assert.False(t, mustTxnStep(t, shard, &txn, &prior))
	assert.False(t, txnBlocks(shard, &txn, &prior)) // May commit again.

	// |msgCh| stalls, and the transaction begins to commit.
	timer.timepoint = faketime(time.Second + minDur + 2)
	assert.True(t, mustTxnStep(t, shard, &txn, &prior))

	assert.True(t, timer.stopped)
	assert.Equal(t, time.Duration(-1), txn.minDur)
	assert.Equal(t, maxDur, txn.maxDur) // Did not reach maxDur.
	assert.NotNil(t, txn.readCh)        // Did not reach maxDur.
	assert.Equal(t, 6, txn.consumedCount)

	verifyStoreAndEchoOut(t, shard, map[string]string{"A": "v", "B": "v", "C": "v", "D": "v"})
	assert.NotZero(t, store.checkpoint.Sources[sourceA.Name].ReadThrough)
	assert.NotEmpty(t, store.checkpoint.AckIntents[echoOut.Name])

	assert.Equal(t, prior.committedAt, faketime(0))
	assert.Equal(t, prior.ackedAt, faketime(0))
	assert.Equal(t, txn.beganAt, faketime(time.Second))
	assert.Equal(t, txn.stalledAt, faketime(time.Second+minDur+2))
	assert.Equal(t, txn.prepareBeganAt, faketime(time.Second+minDur+2))
	assert.Equal(t, txn.prepareDoneAt, faketime(time.Second+minDur+2))
}

func TestRunTxnsACKsRecoveredCheckpoint(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var cp = playAndComplete(t, shard)
	cp.AckIntents = map[pb.Journal][]byte{
		echoOut.Name: []byte(`{"Key": "recovered fixture"}` + "\n"),
	}

	go func() {
		assert.Equal(t, context.Canceled, errors.Cause(runTransactions(shard, cp, nil, nil)))
	}()

	// Expect the ACK intent fixture is written to |echoOut|.
	var rr = client.NewRetryReader(context.Background(), tf.ajc, pb.ReadRequest{
		Journal: echoOut.Name,
		Block:   true,
	})
	var it = message.NewReadUncommittedIter(rr, new(testApplication).NewMessage)

	var env, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, &testMessage{Key: "recovered fixture"}, env.Message)
}

func TestRunTxnsUpdatesRecordedHints(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp      = playAndComplete(t, shard)
		msgCh   = make(chan readMessage, 1)
		hintsCh = make(chan time.Time, 1)
	)
	startReadingMessages(shard, cp, msgCh)

	go func() {
		assert.Equal(t, context.Canceled, errors.Cause(runTransactions(shard, cp, msgCh, hintsCh)))
	}()
	// Precondition: recorded hints are not set.
	assert.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintPrimaryKey()).Kvs, 0)

	hintsCh <- time.Time{}

	// runTransactions does a non-blocking select of |hintsCh|, so run two txns
	// to ensure that |hintsCh| is selected and the operation has completed.
	runTransaction(tf, shard, map[string]string{"one": ""})
	runTransaction(tf, shard, map[string]string{"two": ""})

	assert.Len(t, etcdGet(t, tf.etcd, shard.Spec().HintPrimaryKey()).Kvs, 1)
}

func TestRunTxnsAppErrorCases(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var (
		cp    = playAndComplete(t, shard)
		msgCh = make(chan readMessage, 1)
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
			fn:           func() { tf.app.beginErr = errors.New("begin error") },
			expectErr:    "app.BeginTxn: begin error",
			expectFinish: false,
		},
	}
	for _, tc := range cases {
		tc.fn()

		_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "val"})
		assert.EqualError(t, runTransactions(shard, cp, msgCh, nil), tc.expectErr)

		if tc.expectFinish {
			<-(<-tf.app.finishedCh).Done()
		}
	}
}

func mustTxnStep(t require.TestingT, s *shard, txn, prior *transaction) bool {
	var done, err = txnStep(s, txn, prior)
	require.NoError(t, err)
	return done
}
