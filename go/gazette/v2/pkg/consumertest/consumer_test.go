package consumertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	gc "github.com/go-check/check"
)

type ConsumerSuite struct{}

func (s *ConsumerSuite) TestConsumeWithHandoff(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Start a broker & create journal fixtures.
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")

	brokertest.CreateJournals(c, broker,
		brokertest.Journal(pb.JournalSpec{
			Name:        "a/journal",
			Replication: 1,
			LabelSet:    pb.MustLabelSet("framing", "json"),
		}),
		brokertest.Journal(pb.JournalSpec{Name: "a/recoverylog"}),
	)

	// Start and serve a consumer, and create a shard fixture.
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var cmr1 = NewConsumer(c, etcd, rjc, testApp{}, "local", "consumer")
	go cmr1.Serve(c, ctx)

	CreateShards(c, cmr1, &consumer.ShardSpec{
		Id:             "a-shard",
		Sources:        []consumer.ShardSpec_Source{{Journal: "a/journal"}},
		RecoveryLog:    "a/recoverylog",
		HintKeys:       []string{"/hints/path"},
		MaxTxnDuration: time.Second,
	})

	// Publish test messages.
	var wc = client.NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})
	var enc = json.NewEncoder(wc)
	c.Check(enc.Encode(testMsg{Key: "the", Value: "quick"}), gc.IsNil)
	c.Check(enc.Encode(testMsg{Key: "brown", Value: "fox"}), gc.IsNil)
	c.Check(wc.Close(), gc.IsNil)

	// Wait for the consumer shard to catch up to written journal content.
	c.Assert(WaitForShards(ctx, rjc, cmr1.Service.Loopback,
		pb.LabelSelector{Include: pb.MustLabelSet("id", "a-shard")}), gc.IsNil)

	// Expect the shard store reflects consumed messages.
	var res, err = cmr1.Service.Resolver.Resolve(consumer.ResolveArgs{Context: ctx, ShardID: "a-shard"})
	c.Check(err, gc.IsNil)
	c.Check(res.Store.(*consumer.JSONFileStore).State, gc.DeepEquals,
		map[string]string{"the": "quick", "brown": "fox"})
	res.Done() // Release resolution.

	cmr1.ZeroShardLimit(c) // |cmr1| signals its desire to exit.

	var cmr2 = NewConsumer(c, etcd, rjc, testApp{}, "other", "consumer")
	go cmr2.Serve(c, ctx)

	// |cmr2| is assigned the shard, completes recovery, and is promoted to
	// primary. |cmr1| is then able to exit.
	cmr1.WaitForExit(c)

	// Publish additional test messages, and wait for them to be consumed.
	wc = client.NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})
	enc = json.NewEncoder(wc)
	c.Check(enc.Encode(testMsg{Key: "the", Value: "replaced value"}), gc.IsNil)
	c.Check(enc.Encode(testMsg{Key: "added", Value: "key"}), gc.IsNil)
	c.Check(wc.Close(), gc.IsNil)
	c.Check(WaitForShards(ctx, rjc, cmr2.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	// Expect the shard store reflects all consumed messages.
	res, err = cmr2.Service.Resolver.Resolve(consumer.ResolveArgs{Context: ctx, ShardID: "a-shard"})
	c.Check(err, gc.IsNil)
	c.Check(res.Store.(*consumer.JSONFileStore).State, gc.DeepEquals,
		map[string]string{"the": "replaced value", "brown": "fox", "added": "key"})
	res.Done() // Release resolution.

	cmr2.RevokeLease(c)
	cmr2.WaitForExit(c)

	broker.RevokeLease(c)
	broker.WaitForExit()
}

func (s *ConsumerSuite) TestConsumeWithHotStandby(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// Start a broker & create journal fixtures.
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")

	brokertest.CreateJournals(c, broker,
		brokertest.Journal(pb.JournalSpec{
			Name:        "a/journal",
			Replication: 1,
			LabelSet:    pb.MustLabelSet("framing", "json"),
		}),
		brokertest.Journal(pb.JournalSpec{Name: "a/recoverylog"}),
	)

	// Start and serve a consumer, and create a shard fixture.
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var cmr1 = NewConsumer(c, etcd, rjc, testApp{}, "local", "consumer")
	go cmr1.Serve(c, ctx)

	CreateShards(c, cmr1, &consumer.ShardSpec{
		Id:             "a-shard",
		Sources:        []consumer.ShardSpec_Source{{Journal: "a/journal"}},
		RecoveryLog:    "a/recoverylog",
		HintKeys:       []string{"/hints/path"},
		MaxTxnDuration: time.Second,
		HotStandbys:    1,
	})

	var cmr2 = NewConsumer(c, etcd, rjc, testApp{}, "other", "consumer")
	go cmr2.Serve(c, ctx)

	<-cmr1.AllocateIdleCh() // |cmr2| is assigned as standby.

	// Publish test messages and wait for the shard to catch up.
	var wc = client.NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})
	var enc = json.NewEncoder(wc)
	c.Check(enc.Encode(testMsg{Key: "the", Value: "quick"}), gc.IsNil)
	c.Check(enc.Encode(testMsg{Key: "brown", Value: "fox"}), gc.IsNil)
	c.Check(wc.Close(), gc.IsNil)

	c.Check(WaitForShards(ctx, rjc, cmr1.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	// Crash |cmr1|.
	cmr1.RevokeLease(c)
	cmr1.WaitForExit(c)

	<-cmr2.AllocateIdleCh() // |cmr2| takes over the shard.

	// Expect shard is served by |cmr2|, which has expected key/values.
	var res, err = cmr2.Service.Resolver.Resolve(consumer.ResolveArgs{Context: ctx, ShardID: "a-shard"})
	c.Check(err, gc.IsNil)
	c.Check(res.Store.(*consumer.JSONFileStore).State, gc.DeepEquals,
		map[string]string{"the": "quick", "brown": "fox"})
	res.Done() // Release resolution.

	cmr2.RevokeLease(c)
	cmr2.WaitForExit(c)

	broker.RevokeLease(c)
	broker.WaitForExit()
}

type testApp struct{}

type testMsg struct{ Key, Value string }

func (testApp) NewMessage(spec *pb.JournalSpec) (message.Message, error) { return new(testMsg), nil }

func (testApp) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var state = make(map[string]string)
	return consumer.NewJSONFileStore(rec, dir, &state)
}

func (testApp) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope) error {
	var js = store.(*consumer.JSONFileStore)
	var msg = env.Message.(*testMsg)
	js.State.(map[string]string)[msg.Key] = msg.Value
	return nil
}

func (testApp) FinalizeTxn(shard consumer.Shard, store consumer.Store) error { return nil }

var _ = gc.Suite(&ConsumerSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
