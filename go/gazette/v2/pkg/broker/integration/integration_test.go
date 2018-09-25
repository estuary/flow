// Package integration is a test-only package of broker integration tests.
package integration

import (
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/broker"
	"github.com/LiveRamp/gazette/v2/pkg/broker/teststub"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	gc "github.com/go-check/check"
)

// TODO(johnny): Add additional integration testing scenarios (Issue #69).

type IntegrationSuite struct{}

func (s *IntegrationSuite) TestBasicReadAndWrite(c *gc.C) {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	defer etcdtest.Cleanup()

	var etcd, session = etcdSession(c)

	var ks = broker.NewKeySpace("/gazette/cluster")
	c.Check(ks.Load(ctx, etcd, 0), gc.IsNil)
	go ks.Watch(ctx, etcd)

	var brokerA = newTestServer(pb.BrokerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id: pb.ProcessSpec_ID{Zone: "A", Suffix: "broker-one"},
		},
		JournalLimit: 5,
	})
	var brokerB = newTestServer(pb.BrokerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id: pb.ProcessSpec_ID{Zone: "B", Suffix: "broker-two"},
		},
		JournalLimit: 5,
	})

	brokerA.start(c, ctx, ks, session, etcd)
	brokerB.start(c, ctx, ks, session, etcd)

	var journal = createTestJournal(c, ks, etcd)

	// TODO(johnny): This is super ugly. Can we use the AllocateArgs hook to
	// drive test stages upon reaching idle-ness?
	time.Sleep(time.Millisecond * 500)

	ctx = pb.WithDispatchDefault(ctx)

	var r = client.NewReader(ctx,
		pb.NewRoutedJournalClient(brokerA.srv.MustClient(), pb.NoopDispatchRouter{}),
		pb.ReadRequest{
			Journal: journal,
			Block:   true,
		})
	var a = client.NewAppender(ctx,
		pb.NewRoutedJournalClient(brokerB.srv.MustClient(), pb.NoopDispatchRouter{}),
		pb.AppendRequest{
			Journal: journal,
		})

	go func() {
		var _, err = a.Write([]byte("hello, world!\nextra"))
		c.Check(err, gc.IsNil)
		c.Check(a.Close(), gc.IsNil)
	}()

	var br = bufio.NewReader(r)
	str, err := br.ReadString('\n')
	c.Check(err, gc.IsNil)
	c.Check(str, gc.Equals, "hello, world!\n")

	// Lower journal replication to one. This allows |brokerA| to exit gracefully.
	setJournalReplication(c, ks, etcd, journal, 1)
	brokerA.gracefulStop(c, ctx)

	// Delete the journal. |brokerB| can now exit.
	_, err = etcd.Delete(context.Background(),
		allocator.ItemKey(ks, journal.String()))
	c.Check(err, gc.IsNil)

	brokerB.gracefulStop(c, ctx)
}

type testBroker struct {
	spec         pb.BrokerSpec
	key          string
	state        *allocator.State
	srv          *teststub.Server
	svc          broker.Service
	announcement *allocator.Announcement
	stopped      chan struct{}
}

func newTestServer(spec pb.BrokerSpec) *testBroker {
	return &testBroker{spec: spec}
}

func (s *testBroker) start(c *gc.C, ctx context.Context, ks *keyspace.KeySpace, session *concurrency.Session, etcd *clientv3.Client) {

	s.key = allocator.MemberKey(ks, s.spec.Id.Zone, s.spec.Id.Suffix)
	s.state = allocator.NewObservedState(ks, s.key)
	s.srv = teststub.NewServer(c, ctx, &s.svc)
	s.svc = *broker.NewService(s.state, s.srv.MustClient(), etcd)
	s.spec.Endpoint = s.srv.Endpoint()
	s.announcement = allocator.Announce(etcd, s.key, s.spec.MarshalString(), session.Lease())
	s.stopped = make(chan struct{})

	go func() {

		// Wait until our member key is reflected in KeySpace, before starting Allocate.
		ks.Mu.RLock()
		c.Check(ks.WaitForRevision(ctx, s.announcement.Revision), gc.IsNil)
		ks.Mu.RUnlock()

		c.Check(allocator.Allocate(allocator.AllocateArgs{
			Context: ctx,
			Etcd:    etcd,
			State:   s.state,
		}), gc.IsNil)
		close(s.stopped)
	}()
}

func (s *testBroker) gracefulStop(c *gc.C, ctx context.Context) {
	s.spec.JournalLimit = 0
	c.Check(s.announcement.Update(s.spec.MarshalString()), gc.IsNil)
	<-s.stopped
}

func createTestJournal(c *gc.C, ks *keyspace.KeySpace, etcd *clientv3.Client) pb.Journal {
	var spec = pb.JournalSpec{
		Name:        "foo/bar",
		Replication: 2,
		Fragment: pb.JournalSpec_Fragment{
			Length:           1 << 16,
			CompressionCodec: pb.CompressionCodec_GZIP,
			RefreshInterval:  time.Second,
		},
		LabelSet: pb.LabelSet{
			Labels: []pb.Label{
				{Name: "label-key", Value: "label-value"},
				{Name: "topic", Value: "foo"},
			},
		},
	}
	c.Check(spec.Validate(), gc.IsNil)

	var resp, err = etcd.Put(context.Background(),
		allocator.ItemKey(ks, spec.Name.String()),
		spec.MarshalString(),
	)
	c.Check(err, gc.IsNil)

	ks.Mu.RLock()
	ks.WaitForRevision(context.Background(), resp.Header.Revision)
	ks.Mu.RUnlock()

	return spec.Name
}

func setJournalReplication(c *gc.C, ks *keyspace.KeySpace, etcd *clientv3.Client, journal pb.Journal, r int32) {
	var spec pb.JournalSpec

	if item, ok := allocator.LookupItem(ks, journal.String()); ok {
		spec = *item.ItemValue.(*pb.JournalSpec)
	} else {
		c.Fatalf("journal not found: %s", journal)
	}

	spec.Replication = r
	var _, err = etcd.Put(context.Background(),
		allocator.ItemKey(ks, spec.Name.String()),
		spec.MarshalString(),
	)
	c.Check(err, gc.IsNil)
}

func etcdSession(c *gc.C) (*clientv3.Client, *concurrency.Session) {
	var etcd = etcdtest.TestClient()

	var session, err = concurrency.NewSession(etcd)
	c.Assert(err, gc.IsNil)

	return etcd, session
}

func Test(t *testing.T) { gc.TestingT(t) }

var _ = gc.Suite(&IntegrationSuite{})
