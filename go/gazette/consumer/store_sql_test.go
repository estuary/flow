package consumer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestSQLCheckpointPersistAndRestore(t *testing.T) {
	var spec = makeRemoteShard(shardA)
	var tf, cleanup = newTestFixture(t)

	tf.allocateShard(spec, localID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	// Run one transaction, then de-assign the shard.
	var res, err = tf.resolver.Resolve(ResolveArgs{Context: context.Background(), ShardID: shardA})
	require.NoError(t, err)

	var fqn = res.Shard.FQN()
	runTransaction(tf, res.Shard, map[string]string{"key": "one"})
	tf.allocateShard(spec)
	res.Done()

	// Expect a checkpoint was persisted.
	var fence int
	require.NoError(t, tf.app.db.QueryRow("SELECT fence FROM gazette_checkpoints "+
		"WHERE shard_fqn=$1", fqn).Scan(&fence))
	require.Equal(t, 1, fence)

	// Re-assign as primary.
	tf.allocateShard(spec, localID)
	expectStatusCode(t, tf.state, pc.ReplicaStatus_PRIMARY)

	// Expect the checkpoint was restored and its fence increased.
	require.NoError(t, tf.app.db.QueryRow("SELECT fence FROM gazette_checkpoints "+
		"WHERE shard_fqn=$1", fqn).Scan(&fence))
	require.Equal(t, 2, fence)

	res, err = tf.resolver.Resolve(ResolveArgs{Context: context.Background(), ShardID: shardA})
	require.NoError(t, err)

	runTransaction(tf, res.Shard, map[string]string{"key": "two"})
	verifyStoreAndEchoOut(t, res.Shard.(*shard), map[string]string{"key": "two"})

	// Increase the fence out-of-band (eg, as another raced primary would).
	_, err = tf.app.db.Exec("UPDATE gazette_checkpoints SET fence = fence + 1")
	require.NoError(t, err)

	runTransaction(tf, res.Shard, map[string]string{"key": "fails"})
	// The actual error message might have a slightly different prefix because the
	// error _might_ get returned synchronously as part of `txnStartCommit` if the
	// operation happens to be comppleted before that function returns. That's why we
	// use `Contains` here.
	require.Contains(t, expectStatusCode(t, tf.state, pc.ReplicaStatus_FAILED).Errors[0],
		"store.StartCommit: checkpoint fence was updated (ie, by a new primary)")

	// Cleanup.
	res.Done()
	tf.allocateShard(spec)
	cleanup()
}

func TestSQLCheckpointInsertRace(t *testing.T) {
	var spec = makeRemoteShard(shardA)
	var tf, cleanup = newTestFixture(t)

	// Install a checkpoint fixture which will appear to indicate there is no checkpoint
	// for this FQN, but results in a constraint violation (this works because SQLStore
	// interprets fence=0 as "does not exist", and always starts fences at 1).
	const fqn = "/consumer.test/items/shard-A"
	var _, err = tf.app.db.Exec("INSERT INTO gazette_checkpoints (shard_fqn, fence, checkpoint) "+
		"VALUES ($1, $2, '')", fqn, -1)
	require.NoError(t, err)

	tf.allocateShard(spec, localID)
	require.Equal(t, "completeRecovery: store.RestoreCheckpoint: UNIQUE constraint failed: gazette_checkpoints.shard_fqn",
		expectStatusCode(t, tf.state, pc.ReplicaStatus_FAILED).Errors[0])

	tf.allocateShard(spec)
	cleanup()
}
