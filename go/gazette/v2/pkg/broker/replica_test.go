package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type ReplicaSuite struct{}

// TODO(johnny): Additional unit-test coverage needed here (issue #68).

func (s *ReplicaSuite) TestSpoolAcquisition(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	var r = newReplica("foobar")

	var spool, err = acquireSpool(ctx, r, false)
	c.Check(err, gc.IsNil)
	c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
		Journal:          "foobar",
		CompressionCodec: pb.CompressionCodec_NONE,
	})

	// A next attempt to acquire the Spool will block until the current instance is returned.
	go func(s fragment.Spool) {
		s.Fragment.Begin = 8
		s.Fragment.End = 8
		s.Fragment.Sum = pb.SHA1Sum{Part1: 01234}
		r.spoolCh <- s
	}(spool)

	spool, err = acquireSpool(ctx, r, false)
	c.Check(err, gc.IsNil)

	c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
		Journal:          "foobar",
		Begin:            8,
		End:              8,
		Sum:              pb.SHA1Sum{Part1: 01234},
		CompressionCodec: pb.CompressionCodec_NONE,
	})
	r.spoolCh <- spool

	// acquireSpool optionally waits on the fragment.Index to load. Further,
	// expect the Spool will roll forward if the Index is aware of a greater offset.

	go func() {
		var set, _ = fragment.CoverSet{}.Add(fragment.Fragment{
			Fragment: pb.Fragment{
				Journal:          "foobar",
				Begin:            10,
				End:              20,
				CompressionCodec: pb.CompressionCodec_NONE,
			},
		})
		r.index.ReplaceRemote(set)
	}()

	spool, err = acquireSpool(ctx, r, true)
	c.Check(err, gc.IsNil)

	c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
		Journal:          "foobar",
		Begin:            20,
		End:              20,
		CompressionCodec: pb.CompressionCodec_NONE,
	})

	// Note |spool| is not returned. Cancel |ctx| in the background.
	go cancel()

	_, err = acquireSpool(ctx, r, false)
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *ReplicaSuite) TestPipelineAcquisition(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)

	var jc = broker.MustClient()
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})

	// Create an "outdated" header having an older revision and a non-equivalent Route.
	var hdr = res.Header
	hdr.Etcd.Revision -= 1
	hdr.Route.Members = []pb.ProcessSpec_ID{{Zone: "different", Suffix: "broker"}, {Zone: "peer", Suffix: "broker"}}

	// Expect no pipeline is returned, and the Revision to read through is.
	var pln, rev, err = acquirePipeline(tf.ctx, res.replica, hdr, jc)
	c.Check(pln, gc.IsNil)
	c.Check(rev, gc.Equals, tf.ks.Header.Revision)
	c.Check(err, gc.IsNil)

	// Expect |readThroughRev| is set on the pipeline.
	pln = <-res.replica.pipelineCh
	c.Check(pln.readThroughRev, gc.Equals, tf.ks.Header.Revision)
	res.replica.pipelineCh <- pln

	// Repeated invocations return |readThroughRev| again.
	_, rev, _ = acquirePipeline(tf.ctx, res.replica, hdr, jc)
	c.Check(rev, gc.Equals, tf.ks.Header.Revision)

	// Try again with a "current" header. It succeeds.
	pln, rev, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(pln, gc.NotNil)
	c.Check(rev, gc.Equals, int64(0))
	c.Check(err, gc.IsNil)

	var didRun bool
	go func() {
		time.Sleep(time.Millisecond)
		didRun = true
		res.replica.pipelineCh <- pln
	}()

	// Next invocation blocks until the pipeline is returned by the last caller.
	pln, rev, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(pln, gc.NotNil)
	c.Check(didRun, gc.Equals, true)
	res.replica.pipelineCh <- pln

	// Introduce a new peer, add a corresponding journal assignment, and re-resolve..
	var peer2 = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker2"}, newReplica)
	mustKeyValues(c, tf, map[string]string{
		allocator.AssignmentKey(tf.ks, allocator.Assignment{
			ItemID:       "a/journal",
			MemberZone:   "peer",
			MemberSuffix: "broker2",
			Slot:         3,
		}): "",
	})
	res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})

	// Expect the pipeline was re-built, using the updated Route.
	pln, rev, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(pln, gc.NotNil)
	c.Check(err, gc.IsNil)
	c.Check(pln.Header.Route.Members, gc.DeepEquals, []pb.ProcessSpec_ID{broker.id, peer.id, peer2.id})

	// Don't return |pln|. Attempts to acquire it block indefinitely, until the context is cancelled.
	go cleanup()

	_, _, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(err, gc.Equals, context.Canceled)
}

var _ = gc.Suite(&ReplicaSuite{})
