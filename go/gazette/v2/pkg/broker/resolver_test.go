package broker

import (
	"context"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type ResolverSuite struct{}

func (s *ResolverSuite) TestResolutionCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var peer = newMockBroker(c, ctx, ks, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})

	var mkRoute = func(i int, b ...*testBroker) (rt pb.Route) {
		rt = pb.Route{Primary: int32(i)}
		for i := range b {
			rt.Members = append(rt.Members, b[i].id)
			rt.Endpoints = append(rt.Endpoints, b[i].Endpoint())
		}
		return
	}
	newTestJournal(c, ks, "primary/journal", 2, broker.id, peer.id)
	newTestJournal(c, ks, "replica/journal", 2, peer.id, broker.id)
	newTestJournal(c, ks, "no/primary/journal", 2, pb.ProcessSpec_ID{}, broker.id, peer.id)
	newTestJournal(c, ks, "no/brokers/journal", 2)
	newTestJournal(c, ks, "insufficient/brokers/journal", 3, broker.id, peer.id)
	newTestJournal(c, ks, "peer/only/journal", 1, peer.id)

	// Expect a replica was created for each journal |broker| is responsible for.
	c.Check(broker.resolver.replicas, gc.HasLen, 4)

	// Case: simple resolution of local replica.
	var r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "replica/journal"})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	// Expect the local replica is attached.
	c.Check(r.replica, gc.Equals, broker.resolver.replicas["replica/journal"])
	// As is the JournalSpec
	c.Check(r.journalSpec.Name, gc.Equals, pb.Journal("replica/journal"))
	// And a Header having the correct Route (with Endpoints), Etcd header, and responsible broker ID.
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(1, broker, peer))
	c.Check(r.Header.Etcd, gc.DeepEquals, pb.FromEtcdResponseHeader(ks.Header))

	// Case: primary is required, and we are primary.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "primary/journal", requirePrimary: true})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(0, broker, peer))

	// Case: primary is required, we are not primary, and may not proxy.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "replica/journal", requirePrimary: true})
	c.Check(r.status, gc.Equals, pb.Status_NOT_JOURNAL_PRIMARY_BROKER)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id) // Still |broker|, since it authored the response.
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(1, broker, peer))
	c.Check(r.replica, gc.IsNil)

	// Case: primary is required, and we may proxy.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "replica/journal", requirePrimary: true, mayProxy: true})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	c.Check(r.Header.ProcessId, gc.Equals, peer.id) // This time, |peer| is the resolved broker.
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(1, broker, peer))

	// Case: primary is required, we may proxy, but there is no primary.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "no/primary/journal", requirePrimary: true, mayProxy: true})
	c.Check(r.status, gc.Equals, pb.Status_NO_JOURNAL_PRIMARY_BROKER)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(-1, broker, peer))

	// Case: we may not proxy, and are not a replica.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "peer/only/journal"})
	c.Check(r.status, gc.Equals, pb.Status_NOT_JOURNAL_BROKER)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(0, peer))

	// Case: we may proxy, and are not a replica.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "peer/only/journal", mayProxy: true})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	c.Check(r.Header.ProcessId, gc.Equals, pb.ProcessSpec_ID{}) // Primary not required and non-local => no ID.
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(0, peer))

	// Case: we require the journal be fully assigned, and it is.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "replica/journal", requireFullAssignment: true})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(1, broker, peer))

	// Case: we require the journal be fully assigned, and it isn't.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "insufficient/brokers/journal", requireFullAssignment: true})
	c.Check(r.status, gc.Equals, pb.Status_INSUFFICIENT_JOURNAL_BROKERS)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(0, broker, peer))

	// Case: the journal has no brokers.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "no/brokers/journal", mayProxy: true})
	c.Check(r.status, gc.Equals, pb.Status_INSUFFICIENT_JOURNAL_BROKERS)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(-1))

	// Case: the journal doesn't exist.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "does/not/exist"})
	c.Check(r.status, gc.Equals, pb.Status_JOURNAL_NOT_FOUND)
	c.Check(r.Header.ProcessId, gc.Equals, broker.id)
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(-1))

	// Case: our broker key has been removed.
	c.Check(ks.Apply(etcdEvent(ks, "del", broker.state.LocalKey, "")), gc.IsNil)

	// Subcase 1: We can still resolve for peer journals.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "peer/only/journal", mayProxy: true})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	c.Check(r.Header.ProcessId, gc.Equals, pb.ProcessSpec_ID{})
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(0, peer))

	// Subcase 2: We use a placeholder ProcessId.
	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "peer/only/journal"})
	c.Check(r.status, gc.Equals, pb.Status_NOT_JOURNAL_BROKER)
	c.Check(r.Header.ProcessId, gc.Equals, pb.ProcessSpec_ID{Zone: "local BrokerSpec", Suffix: "missing from Etcd"})
	c.Check(r.Header.Route, gc.DeepEquals, mkRoute(0, peer))
}

func (s *ResolverSuite) TestFutureRevisionCasesWithProxyHeader(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	// Case: Request a resolution, passing along a proxyHeader fixture which
	// references a future Etcd Revision. In the background, arrange for that Etcd
	// update to be delivered.

	var hdr = pb.Header{
		ProcessId: broker.id,
		Route: pb.Route{
			Members:   []pb.ProcessSpec_ID{broker.id},
			Primary:   0,
			Endpoints: []pb.Endpoint{broker.Endpoint()},
		},
		Etcd: pb.FromEtcdResponseHeader(ks.Header),
	}
	hdr.Etcd.Revision += 1

	go func() {
		time.Sleep(time.Millisecond)
		newTestJournal(c, ks, "journal/one", 1, broker.id)
	}()

	// Expect the resolution succeeds, despite the journal not yet existing.
	var r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "journal/one", proxyHeader: &hdr})
	c.Check(r.status, gc.Equals, pb.Status_OK)
	c.Check(r.Header, gc.DeepEquals, hdr)

	// Case: this time, specify a future revision via |minEtcdRevision|. Expect that also works.
	go func() {
		time.Sleep(time.Millisecond)
		newTestJournal(c, ks, "journal/two", 1, broker.id)
	}()

	r, _ = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "journal/two", minEtcdRevision: ks.Header.Revision + 1})
	c.Check(r.status, gc.Equals, pb.Status_OK)

	// Case: finally, specify a future revision which doesn't come about and cancel the context.
	go cancel()

	var _, err = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "journal/three", minEtcdRevision: ks.Header.Revision + 1})
	c.Check(err, gc.Equals, context.Canceled)
}

func (s *ResolverSuite) TestProxyHeaderErrorCases(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewKeySpace("/root")
	var broker = newTestBroker(c, ctx, ks, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	var proxy = pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "other", Suffix: "id"},
		Route:     pb.Route{Primary: -1},
		Etcd:      pb.FromEtcdResponseHeader(ks.Header),
	}

	// Case: proxy header references a broker other than this one.
	var _, err = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "a/journal", proxyHeader: &proxy})
	c.Check(err, gc.ErrorMatches, `proxied request ProcessId doesn't match our own \(zone.*`)
	proxy.ProcessId = broker.id

	// Case: proxy header references a ClusterId other than our own.
	proxy.Etcd.ClusterId = 8675309
	_, err = broker.resolver.resolve(resolveArgs{ctx: ctx, journal: "a/journal", proxyHeader: &proxy})
	c.Check(err, gc.ErrorMatches, `proxied request Etcd ClusterId doesn't match our own \(\d+.*`)
}

var _ = gc.Suite(&ResolverSuite{})
