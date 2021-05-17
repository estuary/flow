package fragment

import (
	"context"
	"errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"sync"
	"testing"
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
)

type PersisterSuite struct{}

func (p *PersisterSuite) TestSpoolCompleteNonPrimary(c *gc.C) {
	var persister = NewPersister(nil)
	persister.persistFn = func(context.Context, Spool, *pb.JournalSpec) error {
		c.Error("spool should not be persisted")
		return nil
	}
	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)

	// Spools with no content should not be enqueued.
	persister.SpoolComplete(spool, false)
	persister.mu.Lock()
	c.Check(len(persister.qC), gc.Equals, 0)
	persister.mu.Unlock()

	// Enqueue spools with content on a non-primary node.
	applyAndCommit(&spool)
	persister.SpoolComplete(spool, false)
	persister.mu.Lock()
	c.Check(len(persister.qC), gc.Equals, 1)
	persister.mu.Unlock()
}

func (p *PersisterSuite) TestSpoolCompletePrimary(c *gc.C) {
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
	c.Assert(err, gc.IsNil)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var persister = NewPersister(ks)
	persister.persistFn = func(ctx context.Context, spool Spool, spec *pb.JournalSpec) error {
		c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
			Journal:          "journal-1",
			Begin:            0,
			End:              12,
			Sum:              pb.SHA1SumOf("some content"),
			CompressionCodec: pb.CompressionCodec_NONE,
		})
		c.Check(spec, gc.Equals, specFixture)
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

func (p *PersisterSuite) TestAttempPersistFragmentDropped(c *gc.C) {
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
	c.Assert(err, gc.IsNil)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var persister = Persister{
		doneCh: make(chan struct{}),
		ks:     ks,
		persistFn: func(context.Context, Spool, *pb.JournalSpec) error {
			c.Error("spool should not be persisted")
			return nil
		},
	}

	var obv testSpoolObserver
	var spool = NewSpool("journal-1", &obv)
	// Spool with no content should not call persistFn or enqueue spool.
	persister.attemptPersist(spool)
	persister.mu.Lock()
	c.Check(len(persister.qC), gc.Equals, 0)
	persister.mu.Unlock()

	applyAndCommit(&spool)

	// Journal spec which has been removed should not call persistFn or enqueue spool.
	spool.Journal = pb.Journal("invalidJournal")
	persister.attemptPersist(spool)
	persister.mu.Lock()
	c.Check(len(persister.qC), gc.Equals, 0)
	persister.mu.Unlock()
}

func (p *PersisterSuite) TestServeUpdateBackingStore(c *gc.C) {
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
	c.Assert(err, gc.IsNil)
	c.Check(ks.Load(ctx, client, 0), gc.IsNil)

	var timeChan = make(chan time.Time)
	var ticker = &time.Ticker{C: timeChan}
	ticker.C = timeChan
	var persister = Persister{
		doneCh: make(chan struct{}),
		ks:     ks,
		persistFn: func(ctx context.Context, spool Spool, spec *pb.JournalSpec) error {
			if spec.Fragment.Stores[0] == pb.FragmentStore("file:///root/invalid/") {
				return errors.New("something has gone wrong")
			}
			c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
				Journal:          "journal-1",
				Begin:            0,
				End:              12,
				Sum:              pb.SHA1SumOf("some content"),
				CompressionCodec: pb.CompressionCodec_NONE,
			})
			c.Check(spec, gc.Equals, specFixture)
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
	c.Check(len(persister.qA)+len(persister.qB)+len(persister.qC), gc.Equals, 1)
	persister.mu.Unlock()

	// Update Jouranl Spec with valid store and retry persisting fragment.
	specFixture.Fragment.Stores[0] = "file:///root/"
	timeChan <- time.Time{}
	persister.Finish()
	persister.mu.Lock()
	c.Check(len(persister.qA)+len(persister.qB)+len(persister.qC), gc.Equals, 0)
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

var _ = gc.Suite(&PersisterSuite{})

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
