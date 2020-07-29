package consumertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
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
			LabelSet:    pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
		}),
		brokertest.Journal(pb.JournalSpec{
			Name:     "recovery/logs/a-shard",
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
		}),
	)

	// Start and serve a consumer, and create a shard fixture.
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var cmr1 = NewConsumer(Args{
		C:        c,
		Etcd:     etcd,
		Journals: rjc,
		App:      testApp{},
		Zone:     "zone-1",
	})
	cmr1.Tasks.GoRun()

	CreateShards(c, cmr1, &pc.ShardSpec{
		Id:                "a-shard",
		Sources:           []pc.ShardSpec_Source{{Journal: "a/journal?with=query"}},
		RecoveryLogPrefix: "recovery/logs",
		HintPrefix:        "/hints",
		HintBackups:       1,
		MaxTxnDuration:    time.Second,
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
		&map[string]string{"the": "quick", "brown": "fox"})
	res.Done() // Release resolution.

	cmr1.Signal() // |cmr1| signals its desire to exit.

	var cmr2 = NewConsumer(Args{
		C:        c,
		Etcd:     etcd,
		Journals: rjc,
		App:      testApp{},
		Zone:     "zone-2",
	})
	cmr2.Tasks.GoRun()

	// |cmr1|, which is allocation leader, assigns |cmr2| as standby.
	// |cmr2| becomes ready and is promoted to primary, after which |cmr1|
	// has no more assignments and is able to exit.
	c.Check(cmr1.Tasks.Wait(), gc.IsNil)

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
		&map[string]string{"the": "replaced value", "brown": "fox", "added": "key"})
	res.Done() // Release resolution.

	cmr2.Tasks.Cancel()
	c.Check(cmr2.Tasks.Wait(), gc.IsNil)

	broker.Tasks.Cancel()
	c.Check(broker.Tasks.Wait(), gc.IsNil)
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
			LabelSet:    pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
		}),
		brokertest.Journal(pb.JournalSpec{
			Name:     "recovery/logs/a-shard",
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
		}),
	)

	// Start and serve a consumer, and create a shard fixture.
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var cmr1 = NewConsumer(Args{
		C:        c,
		Etcd:     etcd,
		Journals: rjc,
		App:      testApp{},
		Zone:     "zone-1",
	})
	cmr1.Tasks.GoRun()

	CreateShards(c, cmr1, &pc.ShardSpec{
		Id:                "a-shard",
		Sources:           []pc.ShardSpec_Source{{Journal: "a/journal"}},
		RecoveryLogPrefix: "recovery/logs",
		HintPrefix:        "/hints",
		HintBackups:       1,
		MaxTxnDuration:    time.Second,
		HotStandbys:       1,
	})

	var cmr2 = NewConsumer(Args{
		C:        c,
		Etcd:     etcd,
		Journals: rjc,
		App:      testApp{},
		Zone:     "zone-2",
	})
	cmr2.Tasks.GoRun()

	// |cmr2| is assigned as standby.

	// Publish test messages and wait for the shard to catch up.
	var wc = client.NewAppender(ctx, rjc, pb.AppendRequest{Journal: "a/journal"})
	var enc = json.NewEncoder(wc)
	c.Check(enc.Encode(testMsg{Key: "the", Value: "quick"}), gc.IsNil)
	c.Check(enc.Encode(testMsg{Key: "brown", Value: "fox"}), gc.IsNil)
	c.Check(wc.Close(), gc.IsNil)

	c.Check(WaitForShards(ctx, rjc, cmr1.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	// Crash |cmr1|.
	cmr1.Tasks.Cancel()
	c.Check(cmr1.Tasks.Wait(), gc.IsNil)

	// |cmr2| takes over the shard.
	c.Check(cmr2.WaitForPrimary(ctx, "a-shard", nil), gc.IsNil)

	// Expect shard is served by |cmr2|, which has expected key/values.
	var res, err = cmr2.Service.Resolver.Resolve(consumer.ResolveArgs{Context: ctx, ShardID: "a-shard"})
	c.Check(err, gc.IsNil)
	c.Check(res.Store.(*consumer.JSONFileStore).State, gc.DeepEquals,
		&map[string]string{"the": "quick", "brown": "fox"})
	res.Done() // Release resolution.

	cmr2.Tasks.Cancel()
	c.Check(cmr2.Tasks.Wait(), gc.IsNil)

	broker.Tasks.Cancel()
	c.Check(broker.Tasks.Wait(), gc.IsNil)
}

type testApp struct{}

type testMsg struct {
	UUID       message.UUID
	Key, Value string
}

func (m *testMsg) GetUUID() message.UUID                         { return m.UUID }
func (m *testMsg) SetUUID(uuid message.UUID)                     { m.UUID = uuid }
func (m *testMsg) NewAcknowledgement(pb.Journal) message.Message { return new(testMsg) }

func (testApp) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var state = make(map[string]string)
	return consumer.NewJSONFileStore(rec, &state)
}

func (testApp) NewMessage(spec *pb.JournalSpec) (message.Message, error) { return new(testMsg), nil }

func (testApp) ConsumeMessage(_ consumer.Shard, store consumer.Store, env message.Envelope, _ *message.Publisher) error {
	var js = store.(*consumer.JSONFileStore)
	var msg = env.Message.(*testMsg)
	(*js.State.(*map[string]string))[msg.Key] = msg.Value
	return nil
}

func (testApp) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error { return nil }

var _ = gc.Suite(&ConsumerSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
