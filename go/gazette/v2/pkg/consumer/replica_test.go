package consumer

import (
	"errors"

	gc "github.com/go-check/check"
)

type ReplicaSuite struct{}

func (s *ReplicaSuite) TestStandbyToPrimaryTransitions(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	// Begin as a standby replica of the shard.
	tf.allocateShard(c, makeShard("a-shard"), remoteID, localID)

	// Expect that status transitions to BACKFILL, then to TAILING.
	expectStatusCode(c, tf.state, ReplicaStatus_BACKFILL)
	expectStatusCode(c, tf.state, ReplicaStatus_TAILING)

	// Re-assign as shard primary.
	tf.allocateShard(c, makeShard("a-shard"), localID)

	// Expect that status now transitions to PRIMARY.
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	// Verify message pump and consumer loops were started.
	var r = tf.resolver.replicas["a-shard"]
	runSomeTransactions(c, r, r.app.(*testApplication), r.store.(*JSONFileStore))

	tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
}

func (s *ReplicaSuite) TestDirectToPrimaryTransition(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	tf.allocateShard(c, makeShard("a-shard"), localID)

	// Expect that status transitions to PRIMARY.
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	// Verify message pump and consumer loops were started.
	var r = tf.resolver.replicas["a-shard"]
	runSomeTransactions(c, r, r.app.(*testApplication), r.store.(*JSONFileStore))

	tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
}

func (s *ReplicaSuite) TestPlayRecoveryLogError(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var shard = makeShard("a-shard")
	shard.RecoveryLog = "does/not/exist"
	tf.allocateShard(c, shard, remoteID, localID)

	// Expect that status transitions to FAILED, with a descriptive error.
	c.Check(expectStatusCode(c, tf.state, ReplicaStatus_FAILED).Errors[0],
		gc.Matches, `playLog: playing log does/not/exist: determining log head: JOURNAL_NOT_FOUND`)

	tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
}

func (s *ReplicaSuite) TestCompletePlaybackError(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	tf.app.newStoreErr = errors.New("an error") // Cause NewStore to fail.
	tf.allocateShard(c, makeShard("a-shard"), localID)

	c.Check(expectStatusCode(c, tf.state, ReplicaStatus_FAILED).Errors[0],
		gc.Matches, `completePlayback: initializing store: an error`)

	tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
}

func (s *ReplicaSuite) TestPumpMessagesError(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var shard = makeShard("a-shard")
	shard.Sources[1].Journal = "xxx/does/not/exist"
	tf.allocateShard(c, shard, localID)

	// Expect that status transitions to FAILED, with a descriptive error.
	c.Check(expectStatusCode(c, tf.state, ReplicaStatus_FAILED).Errors[0],
		gc.Matches, `pumpMessages: fetching JournalSpec: named journal does not exist \(xxx/does/not/exist\)`)

	tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
}

func (s *ReplicaSuite) TestConsumeMessagesErrors(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var cases = []struct {
		fn     func()
		expect string
	}{
		// Case: Consume() fails.
		{
			func() { tf.app.consumeErr = errors.New("an error") },
			`consumeMessages: txnStep: app.ConsumeMessage: an error`,
		},
		// Case: FinishTxn() fails.
		{
			func() { tf.app.finishErr = errors.New("an error") },
			`consumeMessages: FinishTxn: an error`,
		},
		// Case: Both fail. Consume()'s error dominates.
		{
			func() {
				tf.app.consumeErr = errors.New("an error")
				tf.app.finishErr = errors.New("shadowed error")
			},
			`consumeMessages: txnStep: app.ConsumeMessage: an error`,
		},
	}
	for _, tc := range cases {
		tf.app.consumeErr, tf.app.finishErr = nil, nil // Reset fixture.

		tf.allocateShard(c, makeShard("a-shard"), localID)
		expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

		var r = tf.resolver.replicas["a-shard"]
		runSomeTransactions(c, r, r.app.(*testApplication), r.store.(*JSONFileStore))

		// Set failure fixture, and write a message to trigger it.
		tc.fn()

		var aa = r.JournalClient().StartAppend(sourceB)
		_, _ = aa.Writer().WriteString(`{"key":"foo"}` + "\n")
		c.Check(aa.Release(), gc.IsNil)

		c.Check(expectStatusCode(c, tf.state, ReplicaStatus_FAILED).Errors[0], gc.Matches, tc.expect)
		tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
	}
}

var _ = gc.Suite(&ReplicaSuite{})
