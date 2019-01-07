package word_count

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/LiveRamp/gazette/v2/cmd/run-consumer/consumermodule"
	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumertest"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type WordCountSuite struct{}

func (s *WordCountSuite) TestPublishAndQuery(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var testJournals, testShards = buildSpecFixtures(4)

	// Start a broker & create journal fixtures.
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")
	brokertest.CreateJournals(c, broker, testJournals...)

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})

	// Start and serve a consumer, and create shard fixtures.
	var ctx, cancel = context.WithCancel(pb.WithDispatchDefault(context.Background()))
	defer cancel()

	var cfg = new(counterConfig)
	var app = new(Counter)
	cfg.WordCount.N = 2

	var cmr = consumertest.NewConsumer(c, etcd, rjc, app, "local", "consumer")
	c.Assert(Counter{}.InitModule(consumermodule.InitArgs{
		Context:     ctx,
		Config:      cfg,
		Application: app,
		Server:      cmr.Server,
		Service:     cmr.Service,
	}), gc.IsNil)
	go cmr.Serve(c, ctx)
	consumertest.CreateShards(c, cmr, testShards...)

	// Publish text to the word-count API.
	var ngc = NewNGramClient(cmr.Service.Loopback)
	var _, err = ngc.Publish(ctx, &PublishRequest{
		Text: "How much wood would a wood chuck chuck if a wood chuck could chuck wood?",
	})
	c.Assert(err, gc.IsNil)

	// Wait for consumer shards to catch up to written topic content.
	c.Assert(consumertest.WaitForShards(ctx, rjc, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	var queryCases = []struct {
		prefix NGram
		shard  consumer.ShardID
		expect []NGramCount
	}{
		// Cases: point lookups which are implicitly resolved to appropriate shards.
		{prefix: "START how",
			expect: []NGramCount{{NGram: "START how", Count: 1}}},
		{prefix: "wood chuck",
			expect: []NGramCount{{NGram: "wood chuck", Count: 2}}},
		{prefix: "chuck chuck",
			expect: []NGramCount{{NGram: "chuck chuck", Count: 1}}},
		{prefix: "a wood",
			expect: []NGramCount{{NGram: "a wood", Count: 2}}},
		{prefix: "wood END",
			expect: []NGramCount{{NGram: "wood END", Count: 1}}},

		// Cases: point lookups with an explicit shard.
		{prefix: "START how", shard: testShards[0].Id, expect: nil},
		{prefix: "START how", shard: testShards[1].Id,
			expect: []NGramCount{{NGram: "START how", Count: 1}}},

		// Case: range lookups with an explicit shard.
		{prefix: "wood", shard: testShards[0].Id,
			expect: []NGramCount{{NGram: "wood chuck", Count: 2}}},
		{prefix: "wood", shard: testShards[1].Id,
			expect: []NGramCount{{NGram: "wood END", Count: 1}, {NGram: "wood would", Count: 1}}},
	}
	for _, qc := range queryCases {
		var resp *QueryResponse
		resp, err = ngc.Query(ctx, &QueryRequest{Prefix: qc.prefix, Shard: qc.shard})
		c.Check(err, gc.IsNil)
		c.Check(resp.Grams, gc.DeepEquals, qc.expect)
	}

	// Shutdown.
	cmr.RevokeLease(c)
	cmr.WaitForExit(c)

	broker.RevokeLease(c)
	broker.WaitForExit()
}

func buildSpecFixtures(parts int) (journals []*pb.JournalSpec, shards []*consumer.ShardSpec) {
	for p := 0; p != parts; p++ {
		var (
			part      = fmt.Sprintf("%03d", p)
			deltaName = pb.Journal("deltas/part-" + part)
			hintsPath = "/hints/shard-" + part
			logName   = pb.Journal("logs/part-" + part)
			shardID   = consumer.ShardID("shard-" + part)
		)
		journals = append(journals,
			brokertest.Journal(pb.JournalSpec{
				Name:        deltaName,
				Replication: 1,
				LabelSet:    pb.MustLabelSet("framing", "fixed", "topic", deltasTopicLabel),
			}),
			brokertest.Journal(pb.JournalSpec{Name: logName}),
		)
		shards = append(shards,
			&consumer.ShardSpec{
				Id:             shardID,
				Sources:        []consumer.ShardSpec_Source{{Journal: deltaName}},
				RecoveryLog:    logName,
				HintKeys:       []string{hintsPath},
				MaxTxnDuration: time.Second,
			},
		)
	}
	return
}

var _ = gc.Suite(&WordCountSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
