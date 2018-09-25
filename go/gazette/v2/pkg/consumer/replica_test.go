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

func (s *ReplicaSuite) TestConsumeMessagesError(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	tf.allocateShard(c, makeShard("a-shard"), localID)

	// Expect that status transitions to PRIMARY.
	expectStatusCode(c, tf.state, ReplicaStatus_PRIMARY)

	// Verify message pump and consumer loops were started.
	var r = tf.resolver.replicas["a-shard"]
	runSomeTransactions(c, r, r.app.(*testApplication), r.store.(*JSONFileStore))

	tf.app.consumeErr = errors.New("an error") // Cause ConsumeMessage to fail.

	var aa = r.JournalClient().StartAppend(sourceB)
	aa.Writer().WriteString("{}\n")
	c.Check(aa.Release(), gc.IsNil)

	c.Check(expectStatusCode(c, tf.state, ReplicaStatus_FAILED).Errors[0],
		gc.Matches, `consumeMessages: txnStep: app.ConsumeMessage: an error`)

	tf.allocateShard(c, makeShard("a-shard")) // Cleanup.
}

var _ = gc.Suite(&ReplicaSuite{})
