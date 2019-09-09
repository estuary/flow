package stream_sum

import (
	"context"
	"fmt"
	"testing"
	"time"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
)

type StreamSumSuite struct{}

func (s *StreamSumSuite) TestUpdateSumCases(c *gc.C) {
	var id = prngStreamID()

	for _, tc := range []struct {
		Chunk
		in     Sum
		expect Sum
	}{
		{Chunk{SeqNo: 1, Data: []byte("battery horse")}, Sum{SeqNo: 0, Value: 0x0},
			Sum{SeqNo: 1, Value: 0xae763109c1192ed6}},

		// Expect variations of chunking still produce the same final sum.
		{Chunk{SeqNo: 1, Data: []byte("battery")}, Sum{SeqNo: 0, Value: 0x0},
			Sum{SeqNo: 1, Value: 0x2be55fc66a381c84}},
		{Chunk{SeqNo: 2, Data: []byte(" horse")}, Sum{SeqNo: 1, Value: 0x2be55fc66a381c84},
			Sum{SeqNo: 2, Value: 0xae763109c1192ed6}},
	} {
		tc.Chunk.ID, tc.in.ID, tc.expect.ID = id, id, id

		var done, err = tc.in.Update(tc.Chunk)
		c.Check(done, gc.Equals, false)
		c.Check(err, gc.IsNil)
		c.Check(tc.in, gc.Equals, tc.expect)
	}

	// Expect a mismatched SeqNo produces an error.
	var sum = Sum{ID: id, SeqNo: 2, Value: 0xfeedbeef}
	var done, err = sum.Update(Chunk{ID: id, SeqNo: 4, Data: []byte("foo")})
	c.Check(err, gc.ErrorMatches, "invalid chunk.SeqNo .*")

	// Expect a mismatched ID produces an error.
	_, err = sum.Update(Chunk{ID: prngStreamID(), SeqNo: 3, Data: []byte("foo")})
	c.Check(err, gc.ErrorMatches, "invalid chunk.ID .*")

	// A terminating (empty) Chunk returns |done|.
	done, err = sum.Update(Chunk{ID: id, SeqNo: 3, Data: nil})
	c.Check(err, gc.IsNil)
	c.Check(done, gc.Equals, true)
}

func (s *StreamSumSuite) TestGenerationWithSimpleFixture(c *gc.C) {
	var chunkCh = make(chan Chunk)
	var expectCh = make(chan Sum)

	go generate(1, 2, expectCh, chunkCh)

	// Expect exactly three chunks are generated, with the 3rd being empty (reflecting end-of-stream),
	// and |chunkCh| is closed thereafter (terminating the range).
	var i int
	var expect Sum

	for chunk := range chunkCh {
		if i++; i == 1 {
			expect.ID = chunk.ID
		}

		c.Check(chunk.ID, gc.Equals, expect.ID)
		c.Check(chunk.SeqNo, gc.Equals, i)

		if i != 3 {
			c.Check(chunk.Data, gc.NotNil)
			continue
		}

		// EOF. Expect a sum is emitted for verification.
		c.Check(chunk.Data, gc.IsNil)
		expect = <-expectCh

		c.Check(chunk.ID, gc.Equals, expect.ID)
		c.Check(chunk.SeqNo, gc.Equals, expect.SeqNo)

		// Expect |expectCh| was closed.
		var _, ok = <-expectCh
		c.Check(ok, gc.Equals, false)
	}
}

func (s *StreamSumSuite) TestGeneratePumpAndVerify(c *gc.C) {
	const nStreams, nChunks = 20, 20

	// Wire together each of |generate|, |verify|, and |pumpSums|. Use a tee and
	// io.Pipe to duplex sums for verification to |teeCh| and the |pumpSums| reader.
	var chunkCh = make(chan Chunk)
	var teeCh = make(chan Sum)
	var verifyCh = make(chan Sum)
	var actualCh = make(chan Sum)

	go func(in <-chan Sum, outs ...chan<- Sum) {
		for sum := range in {
			for _, out := range outs {
				out <- sum
			}
		}
		for _, out := range outs {
			close(out)
		}
	}(teeCh, verifyCh, actualCh)

	go generate(nStreams, nChunks, teeCh, chunkCh)

	var allStreams = make(map[StreamID]struct{})
	var allChunks int

	verify(func(chunk Chunk) {
		allStreams[chunk.ID] = struct{}{}
		allChunks++
	}, chunkCh, verifyCh, actualCh, time.Minute)

	// Expect we saw |nStreams| streams each with |nChunks| chunks (plus 1 for EOF).
	c.Check(allStreams, gc.HasLen, nStreams)
	c.Check(allChunks, gc.Equals, nStreams*(nChunks+1))
}

func (s *StreamSumSuite) TestEndToEnd(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var testJournals, testShards = buildSpecFixtures(4)

	// Start a broker & create journal fixtures.
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	brokertest.CreateJournals(c, broker, testJournals...)

	// Start and serve a consumer, and create shard fixtures.
	var cmr = consumertest.NewConsumer(consumertest.Args{
		C:        c,
		Etcd:     etcd,
		Journals: rjc,
		App:      Summer{},
	})
	cmr.Tasks.GoRun()

	consumertest.CreateShards(c, cmr, testShards...)

	var cfg ChunkerConfig
	cfg.Broker.Address = broker.Endpoint()
	cfg.Chunker.Streams = 10
	cfg.Chunker.Chunks = 10
	cfg.Chunker.Delay = time.Minute

	var ctx, cancel = context.WithCancel(pb.WithDispatchDefault(context.Background()))
	c.Check(GenerateAndVerifyStreams(ctx, &cfg), gc.IsNil)

	// Shutdown.
	cancel()
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
	broker.Tasks.Cancel()
	c.Check(broker.Tasks.Wait(), gc.IsNil)
}

func buildSpecFixtures(parts int) (journals []*pb.JournalSpec, shards []*pc.ShardSpec) {
	journals = append(journals,
		brokertest.Journal(pb.JournalSpec{
			Name:        FinalSumsJournal,
			Replication: 1,
			LabelSet: pb.MustLabelSet(
				labels.MessageType, "Sum",
				labels.ContentType, labels.ContentType_JSONLines,
			),
		}),
	)
	for p := 0; p != parts; p++ {
		var (
			part  = fmt.Sprintf("%03d", p)
			shard = &pc.ShardSpec{
				Id:                pc.ShardID("shard-" + part),
				Sources:           []pc.ShardSpec_Source{{Journal: pb.Journal("chunks/part-" + part)}},
				RecoveryLogPrefix: "recovery/logs",
				HintPrefix:        "/hints",
				MaxTxnDuration:    time.Second,
			}
		)
		journals = append(journals,
			brokertest.Journal(pb.JournalSpec{
				Name:        shard.Sources[0].Journal,
				Replication: 1,
				LabelSet: pb.MustLabelSet(
					labels.MessageType, "Chunk",
					labels.ContentType, labels.ContentType_JSONLines,
				),
			}),
			brokertest.Journal(pb.JournalSpec{
				Name:     shard.RecoveryLog(),
				LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
			}),
		)
		shards = append(shards, shard)
	}
	return
}

var _ = gc.Suite(&StreamSumSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
