package runtime

import (
	"context"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
)

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }

// TestSignalOnSpecUpdateIdentityRePut is the regression test for issue #3247:
// a byte-identical re-PUT of the ShardSpec (as the control plane's activation
// path can emit) must NOT cancel a live task term.
func TestSignalOnSpecUpdateIdentityRePut(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx, ks, key, specBytes = startWatchedShard(t, etcd)

	var termCtx, termCancel = context.WithCancel(ctx)
	go signalOnSpecUpdate(termCtx, termCancel, ks, key, specBytes)

	// Re-PUT identical value bytes twice. Awaiting a revision proves the
	// KeySpace applied it but not that the signal goroutine re-checked it; the
	// second awaited round-trip gives the goroutine a full etcd RPC of
	// scheduling grace to (wrongly) react to the first.
	putAndAwait(t, ctx, etcd, ks, key, string(specBytes))
	putAndAwait(t, ctx, etcd, ks, key, string(specBytes))

	// The term must still be live: identical bytes are not a spec change.
	require.NoError(t, termCtx.Err())

	// Then PUT different bytes and require the term end, proving the goroutine
	// was watching throughout and not parked on a revision it never received.
	var next = makeSignalShardSpec(2 * time.Millisecond)
	var nextBytes, err = next.Marshal()
	require.NoError(t, err)
	_, err = etcd.Put(ctx, key, string(nextBytes))
	require.NoError(t, err)

	requireDone(t, termCtx, "changed spec bytes did not cancel the term")
}

// TestSignalOnSpecUpdateChangedBytes asserts a genuine spec change ends the term.
func TestSignalOnSpecUpdateChangedBytes(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx, ks, key, specBytes = startWatchedShard(t, etcd)

	var termCtx, termCancel = context.WithCancel(ctx)
	go signalOnSpecUpdate(termCtx, termCancel, ks, key, specBytes)

	// PUT a materially different ShardSpec value.
	var next = makeSignalShardSpec(2 * time.Millisecond)
	var nextBytes, err = next.Marshal()
	require.NoError(t, err)
	_, err = etcd.Put(ctx, key, string(nextBytes))
	require.NoError(t, err)

	requireDone(t, termCtx, "changed spec bytes did not cancel the term")
}

// TestSignalOnSpecUpdateDeletion asserts deleting the key ends the term.
func TestSignalOnSpecUpdateDeletion(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx, ks, key, specBytes = startWatchedShard(t, etcd)

	var termCtx, termCancel = context.WithCancel(ctx)
	go signalOnSpecUpdate(termCtx, termCancel, ks, key, specBytes)

	var _, err = etcd.Delete(ctx, key)
	require.NoError(t, err)

	requireDone(t, termCtx, "deleting the shard spec did not cancel the term")
}

// startWatchedShard writes a fresh ShardSpec into Etcd, loads and watches a
// consumer KeySpace over it, and returns a cancelable context, the KeySpace,
// the shard's item key, and the raw value bytes which bind a term.
func startWatchedShard(t *testing.T, etcd *clientv3.Client) (context.Context, *keyspace.KeySpace, string, []byte) {
	var ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var ks = consumer.NewKeySpace("/signal/test")
	var spec = makeSignalShardSpec(1 * time.Millisecond)
	var key = allocator.ItemKey(ks, spec.Id.String())

	var value, err = spec.Marshal()
	require.NoError(t, err)
	_, err = etcd.Put(ctx, key, string(value))
	require.NoError(t, err)

	require.NoError(t, ks.Load(ctx, etcd, 0))
	go func() { _ = ks.Watch(ctx, etcd) }()

	// Capture the value bytes under read lock, exactly as newTaskTerm does.
	var specBytes []byte
	ks.Mu.RLock()
	var ind, ok = ks.Search(key)
	require.True(t, ok)
	specBytes = ks.KeyValues[ind].Raw.Value
	ks.Mu.RUnlock()

	return ctx, ks, key, specBytes
}

// putAndAwait writes value to key, blocking until the KeySpace watch has
// applied the resulting revision. This synchronizes without sleeps.
func putAndAwait(t *testing.T, ctx context.Context, etcd *clientv3.Client, ks *keyspace.KeySpace, key, value string) {
	var resp, err = etcd.Put(ctx, key, value)
	require.NoError(t, err)

	ks.Mu.RLock()
	err = ks.WaitForRevision(ctx, resp.Header.Revision)
	ks.Mu.RUnlock()
	require.NoError(t, err)
}

func requireDone(t *testing.T, ctx context.Context, msg string) {
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal(msg)
	}
}

func makeSignalShardSpec(maxTxn time.Duration) *pf.ShardSpec {
	return &pf.ShardSpec{
		Id:             "acmeCo/test-task/00000000-00000000",
		MaxTxnDuration: maxTxn,
	}
}
