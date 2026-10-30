package fragment

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
)

func TestSpoolCompleteNonPrimary(t *testing.T) {
	var persister = NewPersister(nil)
	persister.persistFn = func(context.Context, Spool, *pb.JournalSpec, bool) error {
		t.Error("spool should not be persisted")
		return nil
	}
	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)

	// Spools with no content should not be enqueued.
	persister.SpoolComplete(spool, false)
	persister.mu.Lock()
	require.Equal(t, 0, len(persister.qC))
	persister.mu.Unlock()

	// Enqueue spools with content on a non-primary node.
	applyAndCommit(&spool)
	persister.SpoolComplete(spool, false)
	persister.mu.Lock()
	require.Equal(t, 1, len(persister.qC))
	persister.mu.Unlock()
}

func TestSpoolCompletePrimary(t *testing.T) {
	var wg sync.WaitGroup
	var specFixture = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores: []pb.FragmentStore{"file:///root/"},
		},
	}
	var ks = keyspace.NewKeySpace("/journals", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{
			ID:        "journal-1",
			ItemValue: specFixture,
		}, nil
	})
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	var _, err = client.Put(ctx, "/journals/items/journal-1", "")
	require.NoError(t, err)
	require.NoError(t, ks.Load(ctx, client, 0))

	var persister = NewPersister(ks)
	persister.persistFn = func(ctx context.Context, spool Spool, spec *pb.JournalSpec, isStopping bool) error {
		require.Equal(t, pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
		}, spool.Fragment.Fragment)
		require.Equal(t, specFixture, spec)
		wg.Done()
		return nil
	}
	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)
	applyAndCommit(&spool)

	wg.Add(1)
	persister.SpoolComplete(spool, true)
	wg.Wait()
}

func TestAttempPersistFragmentDropped(t *testing.T) {
	var specFixture = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores: nil,
		},
	}
	var ks = keyspace.NewKeySpace("/journals", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{
			ID:        "journal-1",
			ItemValue: specFixture,
		}, nil
	})
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	var _, err = client.Put(ctx, "/journals/items/journal-1", "")
	require.NoError(t, err)
	require.NoError(t, ks.Load(ctx, client, 0))

	var persister = Persister{
		doneCh: make(chan struct{}),
		ks:     ks,
		persistFn: func(context.Context, Spool, *pb.JournalSpec, bool) error {
			t.Error("spool should not be persisted")
			return nil
		},
	}

	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)
	// Spool with no content should not call persistFn or enqueue spool.
	persister.attemptPersist(spool, false)
	persister.mu.Lock()
	require.Equal(t, 0, len(persister.qC))
	persister.mu.Unlock()

	applyAndCommit(&spool)

	// Journal spec which has been removed should not call persistFn or enqueue spool.
	spool.Journal = pb.Journal("invalidJournal")
	persister.attemptPersist(spool, false)
	persister.mu.Lock()
	require.Equal(t, 0, len(persister.qC))
	persister.mu.Unlock()
}

func TestServeUpdateBackingStore(t *testing.T) {
	var specFixture = &pb.JournalSpec{
		Fragment: pb.JournalSpec_Fragment{
			Stores: []pb.FragmentStore{"file:///root/invalid/"},
		},
	}
	var ks = keyspace.NewKeySpace("/journals", func(kv *mvccpb.KeyValue) (interface{}, error) {
		return allocator.Item{
			ID:        "journal-1",
			ItemValue: specFixture,
		}, nil
	})
	var client, ctx = etcdtest.TestClient(), context.Background()
	defer etcdtest.Cleanup()
	var _, err = client.Put(ctx, "/journals/items/journal-1", "")
	require.NoError(t, err)
	require.NoError(t, ks.Load(ctx, client, 0))

	var timeChan = make(chan time.Time)
	var ticker = &time.Ticker{C: timeChan}
	ticker.C = timeChan
	var persister = Persister{
		doneCh: make(chan struct{}),
		ks:     ks,
		persistFn: func(ctx context.Context, spool Spool, spec *pb.JournalSpec, isStopping bool) error {
			if spec.Fragment.Stores[0] == pb.FragmentStore("file:///root/invalid/") {
				return errors.New("something has gone wrong")
			}
			require.Equal(t, pb.Fragment{
				Journal:          "journal-1",
				Begin:            0,
				End:              12,
				Sum:              pb.SHA1SumOf("some content"),
				CompressionCodec: pb.CompressionCodec_NONE,
			}, spool.Fragment.Fragment)
			require.Equal(t, specFixture, spec)
			return nil
		},
		ticker: ticker,
	}

	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)
	applyAndCommit(&spool)

	go persister.Serve()
	persister.qA = append(persister.qA, spool)
	// Pass in two time.Time structs to the timeChan to ensure spool has been processed.
	timeChan <- time.Time{}
	timeChan <- time.Time{}
	// Failure to persist should cause spool to be reenqueued, however it is not guaranteed
	// which queue the spool will be in.
	persister.mu.Lock()
	require.Equal(t, 1, len(persister.qA)+len(persister.qB)+len(persister.qC))
	persister.mu.Unlock()

	// Update Jouranl Spec with valid store and retry persisting fragment.
	specFixture.Fragment.Stores[0] = "file:///root/"
	timeChan <- time.Time{}
	persister.Finish()
	persister.mu.Lock()
	require.Equal(t, 0, len(persister.qA)+len(persister.qB)+len(persister.qC))
	persister.mu.Unlock()
}

func applyAndCommit(spool *Spool) {
	spool.applyContent(&pb.ReplicateRequest{
		Content:      []byte("some content"),
		ContentDelta: 0,
	})
	spool.applyCommit(&pb.ReplicateRequest{
		Proposal: &pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
		}}, true)
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
