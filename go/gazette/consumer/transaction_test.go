package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
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
		txn            = transaction{readThrough: make(pb.Offsets)}
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
	require.Equal(t, message.NewClock(txn.beganAt.Add(time.Hour))+1, shard.clock) // Shard clock was updated.

	// Expect it continues to block.
	require.True(t, txnBlocks(shard, &txn, &prior))
	_, _ = tf.pub.PublishCommitted(toSourceA, &testMessage{Key: "key", Value: "2"})
	require.Equal(t, false, mustTxnStep(t, shard, &txn, &prior))

	// Signal that |minDur| has elapsed.
	timer.timepoint = faketime(2*time.Second + minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset) // Was Reset to |maxDur| remainder.

	// Consume additional ready message.
	require.False(t, txnBlocks(shard, &txn, &prior))
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
		txn            = transaction{readThrough: make(pb.Offsets)}
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

	require.True(t, txnBlocks(shard, &txn, &prior)) // Still blocking.

	// |prior| ACKs.
	timer.timepoint = faketime(minDur + 2)
	priorAck.Resolve(nil)
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, prior.ackedAt.IsZero())

	// Consume additional ready message.
	require.False(t, txnBlocks(shard, &txn, &prior))
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
		txn            = transaction{readThrough: make(pb.Offsets)}
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
		txn            = transaction{readThrough: make(pb.Offsets)}
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

	require.True(t, txnBlocks(shard, &txn, &prior))

	// |minDur| has elapsed.
	timer.timepoint = faketime(time.Second + minDur)
	timer.signal()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.Equal(t, maxDur-minDur, timer.reset)     // Was Reset to |maxDur| remainder.
	require.False(t, txnBlocks(shard, &txn, &prior)) // May now commit.

	// Another ready, pending message is read.
	_, _ = tf.pub.PublishUncommitted(toSourceA, &testMessage{Key: "D", Value: "v"})
	msgCh <- <-msgCh // Ensure message is buffered.
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.True(t, txnBlocks(shard, &txn, &prior)) // Wait for ACK.

	// ACK pending message, and read.
	timer.timepoint = faketime(time.Second + minDur + 1)
	tf.writeTxnPubACKs()
	require.False(t, mustTxnStep(t, shard, &txn, &prior))
	require.False(t, txnBlocks(shard, &txn, &prior)) // May commit again.

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

func TestRunTxnsACKsRecoveredCheckpoint(t *testing.T) {
	var tf, shard, cleanup = newTestFixtureWithIdleShard(t)
	defer cleanup()

	var cp = playAndComplete(t, shard)
	cp.AckIntents = map[pb.Journal][]byte{
		echoOut.Name: []byte(`{"Key": "recovered fixture"}` + "\n"),
	}

	// Use a read channel fixture which immediately cancels.
	var readCh = make(chan EnvelopeOrError, 1)
	readCh <- EnvelopeOrError{Error: context.Canceled}
	require.Equal(t, context.Canceled, errors.Cause(runTransactions(shard, cp, readCh, nil)))

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
		require.Equal(t, context.Canceled, errors.Cause(runTransactions(shard, cp, msgCh, hintsCh)))
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
	return done
}
