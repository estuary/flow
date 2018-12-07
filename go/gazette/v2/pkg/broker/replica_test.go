package broker

import (
	"context"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/fragment"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
)

type ReplicaSuite struct{}

func (s *ReplicaSuite) TestSpoolAcquisition(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	var r = newReplica("foobar")

	var spool, err = acquireSpool(ctx, r)
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
		r.spoolCh <- s // Release spool.
	}(spool)

	spool, err = acquireSpool(ctx, r)
	c.Check(err, gc.IsNil)

	c.Check(spool.Fragment.Fragment, gc.DeepEquals, pb.Fragment{
		Journal:          "foobar",
		Begin:            8,
		End:              8,
		Sum:              pb.SHA1Sum{Part1: 01234},
		CompressionCodec: pb.CompressionCodec_NONE,
	})

	// Note |spool| is not returned. Cancel |ctx| in the background.
	go cancel()

	_, err = acquireSpool(ctx, r)
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *ReplicaSuite) TestPipelineAcquisitionAndRelease(c *gc.C) {
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

	// Case: Expect no pipeline is returned, and the Revision to read through is.
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

	// Case: Use a "current" header. It succeeds.
	pln, rev, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(pln, gc.NotNil)
	c.Check(rev, gc.Equals, int64(0))
	c.Check(err, gc.IsNil)

	// Scatter no-op, acknowledged commit.
	pln.scatter(&pb.ReplicateRequest{
		Proposal:    &pln.spool.Fragment.Fragment,
		Acknowledge: true,
	})
	c.Check(pln.sendErr(), gc.IsNil)

	var didRun bool
	go func() {
		time.Sleep(time.Millisecond)
		didRun = true
		c.Check(releasePipelineAndGatherResponse(context.Background(), pln, res.replica.pipelineCh), gc.IsNil)
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

	// Case: Simulate a peer connection error, and scatter a no-op, acknowledged commit.
	pln.streams[2].CloseSend()
	pln.scatter(&pb.ReplicateRequest{
		Proposal:    &pln.spool.Fragment.Fragment,
		Acknowledge: true,
	})
	c.Check(releasePipelineAndGatherResponse(tf.ctx, pln, res.replica.pipelineCh),
		gc.ErrorMatches,
		`send to zone:"peer" suffix:"broker2" : rpc error: code = Internal desc = SendMsg called after CloseSend`)

	// Expect a nil pipeline was released.
	c.Check(<-res.replica.pipelineCh, gc.IsNil)
	res.replica.pipelineCh <- nil

	// Case: Acquisition after error builds a new pipeline.
	pln, rev, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(pln, gc.NotNil)
	c.Check(err, gc.IsNil)

	// Don't return |pln|. Attempts to acquire it block indefinitely, until the context is cancelled.
	go cleanup()

	_, _, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *ReplicaSuite) TestShutdown(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	// Construct a replicated journal fixture, and initialize its pipeline.
	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)

	var jc = broker.MustClient()
	var res, _ = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})

	var pln, _, err = acquirePipeline(tf.ctx, res.replica, res.Header, jc)
	c.Check(pln, gc.NotNil)
	c.Check(err, gc.IsNil)
	res.replica.pipelineCh <- pln // Release.

	shutDownReplica(res.replica)

	// Expect the replica pipeline and spool are torn down, and no longer select-able.
	select {
	case <-res.replica.spoolCh:
		c.Fail()
	case <-res.replica.pipelineCh:
		c.Fail()
	default:
	}
	c.Check(res.replica.ctx.Err(), gc.Equals, context.Canceled)
}

func (s *ReplicaSuite) TestAssignmentUpdateCases(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)

	var res, err = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	c.Check(err, gc.IsNil)

	// Case: assignments have been modified since resolution.
	_, err = tf.etcd.Put(tf.ctx, string(res.assignments[0].Raw.Key), "", clientv3.WithIgnoreLease())
	c.Check(err, gc.IsNil)

	rev, err := updateAssignments(tf.ctx, res.assignments, tf.etcd)
	c.Check(err, gc.IsNil)

	// Expect that updateAssignments left assignments unmodified.
	for _, a := range res.assignments {
		c.Check(a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route),
			gc.DeepEquals, &pb.Route{Primary: -1})
	}

	// Case: assignments haven't been modified since resolution.
	res, err = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", minEtcdRevision: rev})
	c.Check(err, gc.IsNil)

	rev, err = updateAssignments(tf.ctx, res.assignments, tf.etcd)
	c.Check(err, gc.IsNil)

	// Expect that, after resolving at the returned Etcd revision,
	// Etcd assignment routes match the expectation.
	res, err = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", minEtcdRevision: rev})
	c.Check(err, gc.IsNil)

	for _, a := range res.assignments {
		c.Check(a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route),
			gc.DeepEquals, &pb.Route{Members: []pb.ProcessSpec_ID{broker.id, peer.id}})
	}

	// Case: Perform a second assignment update. Expect assignments are not modified.
	rev, err = updateAssignments(tf.ctx, res.assignments, tf.etcd)
	c.Check(err, gc.IsNil)

	res2, err := broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", minEtcdRevision: rev})
	c.Check(err, gc.IsNil)

	// Assignments were not modified, as they were already equivalent.
	c.Check(res.assignments, gc.DeepEquals, res2.assignments)
}

func (s *ReplicaSuite) TestBasicHealthCheck(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	var peer = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"}, newReplica)
	newTestJournal(c, tf, pb.JournalSpec{Name: "a/journal", Replication: 2}, broker.id, peer.id)

	var res, err = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	c.Check(err, gc.IsNil)

	rev, err := checkHealth(res, broker.MustClient(), tf.etcd)
	c.Check(err, gc.IsNil)

	res, err = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal", minEtcdRevision: rev})
	for _, a := range res.assignments {
		c.Check(a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route),
			gc.DeepEquals, &pb.Route{Members: []pb.ProcessSpec_ID{broker.id, peer.id}})
	}
}

func (s *ReplicaSuite) TestHealthCheckWithFlush(c *gc.C) {
	var tf, cleanup = newTestFixture(c)
	defer cleanup()
	defer func(f func() time.Time) { timeNow = f }(timeNow)

	var moreThanSixHoursLater = time.Time{}.Add((time.Hour * 6) + (time.Minute * 10))
	timeNow = func() time.Time { return moreThanSixHoursLater }

	defer func(f func() time.Time) { timeNow = f }(timeNow)
	var flushIntervalSpec = pb.JournalSpec{
		Name:        "a/journal",
		Replication: 1,
		Fragment: pb.JournalSpec_Fragment{
			FlushInterval: time.Duration(time.Hour * 6),
		},
	}

	var broker = newTestBroker(c, tf, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"}, newReadyReplica)
	newTestJournal(c, tf, flushIntervalSpec, broker.id)

	var res, err = broker.resolve(resolveArgs{ctx: tf.ctx, journal: "a/journal"})
	c.Check(err, gc.IsNil)

	// Remove empty spool and insert spool with stubbed observer.
	_ = <-res.replica.spoolCh
	var o = new(testSpoolObserver)
	var spool = fragment.NewSpool("a/journal", o)
	spool.End = 10
	spool.FirstAppendTime = time.Time{}.Add((time.Hour * 6) - (time.Minute * 10))
	spool.Fragment.Sum = pb.SHA1Sum{Part1: 1234}
	res.replica.spoolCh <- spool

	// CheckHealth should prompt a flush of the current fragment. The fragment should found in o.completes.
	_, err = checkHealth(res, broker.MustClient(), tf.etcd)
	c.Check(err, gc.IsNil)
	c.Check(o.completes[0].Sum, gc.DeepEquals, pb.SHA1Sum{Part1: 1234})
}

func (s *ReplicaSuite) TestCheckFragmentFlush(c *gc.C) {
	defer func(f func() time.Time) { timeNow = f }(timeNow)

	var testData = []struct {
		prepArgs    func(fragment.Spool, pb.JournalSpec_Fragment) (fragment.Spool, pb.JournalSpec_Fragment)
		out         pb.Fragment
		description string
	}{
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, pb.JournalSpec_Fragment) {
				spool.End = 100
				spool.FirstAppendTime = time.Time{}.Add(time.Hour * 2)
				spec.Length = 200
				spec.FlushInterval = time.Duration(time.Hour * 6)
				return spool, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				End:              100,
				CompressionCodec: 1,
			},
			description: "Fragment does not need to be flushed",
		},
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, pb.JournalSpec_Fragment) {
				spool.End = 200
				spool.FirstAppendTime = time.Time{}.Add(time.Hour * 2)
				spec.Length = 100
				return spool, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            200,
				End:              200,
				CompressionCodec: 1,
			},
			description: "Fragment exceeds length, get flush proposal",
		},
		{
			prepArgs: func(spool fragment.Spool, spec pb.JournalSpec_Fragment) (fragment.Spool, pb.JournalSpec_Fragment) {
				spool.End = 50
				spool.FirstAppendTime = time.Time{}.Add(time.Minute)
				spec.Length = 100
				spec.FlushInterval = time.Duration(time.Minute * 30)
				return spool, spec
			},
			out: pb.Fragment{
				Journal:          "a/journal",
				Begin:            50,
				End:              50,
				CompressionCodec: 1,
			},
			description: "Fragment contains data from pervious flush interval",
		},
	}

	timeNow = func() time.Time { return time.Time{}.Add(time.Hour) }
	for _, test := range testData {
		var spool, spec = test.prepArgs(
			fragment.NewSpool("a/journal", &testSpoolObserver{}),
			pb.JournalSpec_Fragment{CompressionCodec: 1},
		)
		var proposal = nextProposal(spool, spec)
		c.Log(test.description)
		c.Check(proposal, gc.DeepEquals, test.out)
	}
}

var _ = gc.Suite(&ReplicaSuite{})
