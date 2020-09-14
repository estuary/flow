package shuffle

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/estuary/flow/go/flow"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/jgraettinger/cockroach-encoding/encoding"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/consumertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	gazLabels "go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

func TestConsumerIntegration(t *testing.T) {
	// Fixtures which parameterize the test:
	var (
		sourcePartitions = []pb.Journal{
			"source/foo/part=10",
			"source/foo/part=20",
			"source/foo/part=42",
		}
		shards = []pc.ShardID{
			"derive/bar/abc",
			"derive/bar/def",
		}
		transforms = []pf.TransformSpec{
			{
				Source: pf.TransformSpec_Source{Name: "source/foo"},
				Shuffle: pf.Shuffle{
					Transform:     "highAndLow",
					ShuffleKeyPtr: []string{"/High", "/Low"},
					UsesSourceKey: false,
					FilterRClocks: false,
				},
				Derivation: pf.TransformSpec_Derivation{Name: "derive/bar"},
			},
		}
		N = 200 // Publish each combination of High & Low values two times.
	)

	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Start a flow-worker to serve the extraction RPC.
	wh, err := flow.NewWorkerHost("extract")
	require.Nil(t, err)
	defer wh.Stop()

	// Journals is a consumer-held KeySpace that observes broker-managed journals.
	journals, err := flow.NewJournalsKeySpace(ctx, etcd, "/broker.test")
	require.NoError(t, err)
	journals.WatchApplyDelay = 0
	go journals.Watch(ctx, etcd)

	// Start broker, with journal fixtures.
	var journalSpecs []*pb.JournalSpec
	for _, name := range sourcePartitions {
		journalSpecs = append(journalSpecs, brokertest.Journal(pb.JournalSpec{
			Name: name,
			LabelSet: pb.MustLabelSet(
				gazLabels.ContentType, gazLabels.ContentType_JSONLines,
				flowLabels.Collection, "source/foo",
			),
		}))
	}
	for _, id := range shards {
		journalSpecs = append(journalSpecs, brokertest.Journal(pb.JournalSpec{
			Name:     pb.Journal(fmt.Sprintf("recovery/logs/%s", id)),
			LabelSet: pb.MustLabelSet(gazLabels.ContentType, gazLabels.ContentType_RecoveryLog),
		}))
	}

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	brokertest.CreateJournals(t, broker, journalSpecs...)

	// Write data fixtures randomly across partitions.
	var ajc = client.NewAppendService(ctx, broker.Client())
	var pub = message.NewPublisher(ajc, nil)
	var mapping = func(m message.Mappable) (_ pb.Journal, contentType string, _ error) {
		return sourcePartitions[rand.Intn(len(sourcePartitions))], gazLabels.ContentType_JSONLines, nil
	}

	var expect = make(map[string]int)
	for i := 0; i != N; i++ {
		var msg = &testMsg{
			High: ((i / 10) * 10) % 100, // Takes values [0, 10, 20, 30, ... 90].
			Low:  strconv.Itoa(i % 10),  // Takes values ["0, "1", "2", "3", ... "9"].
		}
		var aa, _ = pub.PublishCommitted(mapping, msg)
		require.NoError(t, aa.Err())

		// Build expected packed shuffle key.
		var k = encoding.EncodeStringAscending(
			encoding.EncodeUvarintAscending(nil, uint64(msg.High)),
			msg.Low)
		expect[string(k)]++
	}
	for op := range ajc.PendingExcept("") {
		require.NoError(t, op.Err())
	}

	// Start consumer, with shard fixtures.
	var c = consumertest.NewConsumer(consumertest.Args{
		C:        t,
		Etcd:     etcd,
		Journals: broker.Client(),
		App: &testApp{
			journals:   journals,
			workerHost: wh,
			transforms: transforms,
		},
	})
	c.Service.App.(*testApp).service = c.Service
	pf.RegisterShufflerServer(c.Server.GRPCServer, &API{resolve: c.Service.Resolver.Resolve})
	c.Tasks.GoRun()

	var shardSpecs = make([]*pc.ShardSpec, len(shards))
	for i, id := range shards {
		var step = 100 / len(shards)

		shardSpecs[i] = &pc.ShardSpec{
			Id:                id,
			Sources:           []pc.ShardSpec_Source{},
			RecoveryLogPrefix: "recovery/logs",
			HintPrefix:        "/hints",
			HintBackups:       1,
			MaxTxnDuration:    time.Second,
			LabelSet: pb.MustLabelSet(
				flowLabels.Derivation, "derived-bar",
				flowLabels.KeyBegin, hex.EncodeToString(encoding.EncodeUvarintAscending(nil, uint64((i+0)*step))),
				flowLabels.KeyEnd, hex.EncodeToString(encoding.EncodeUvarintAscending(nil, uint64((i+1)*step))),
				flowLabels.RClockBegin, "0000000000000000",
				flowLabels.RClockEnd, "ffffffffffffffff",
			),
		}
	}
	consumertest.CreateShards(t, c, shardSpecs...)

	// TODO(johnny): Wait for consumers more elegantly & correctly than this.
	time.Sleep(time.Second)

	// TODO(johnny): We should have some coverage of journal replays.
	// Skipping for now, as it's kind of a "it works or it doesn't"
	// feature which future tests are likely to cover.

	// Pluck out each of the worker states.
	var merged = make(map[string]int)

	for _, id := range shards {
		// Expect the shard store reflects consumed messages.
		res, err := c.Service.Resolver.Resolve(consumer.ResolveArgs{Context: ctx, ShardID: id})
		require.NoError(t, err)

		var state = res.Store.(*testStore).JSONFileStore.State.(map[string]int)
		for k, c := range state {
			merged[k] += c
		}
		res.Done() // Release resolution.
	}
	require.Equal(t, expect, merged)

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

type testApp struct {
	service    *consumer.Service
	journals   *keyspace.KeySpace
	workerHost *flow.WorkerHost
	transforms []pf.TransformSpec
}

type testStore struct {
	*consumer.JSONFileStore
	readBuilder *ReadBuilder
	coordinator *coordinator
}

func (s *testStore) Coordinator() *coordinator { return s.coordinator }

func (a testApp) NewStore(shard consumer.Shard, recorder *recoverylog.Recorder) (consumer.Store, error) {
	var store, err = consumer.NewJSONFileStore(recorder, make(map[string]int))
	return &testStore{JSONFileStore: store}, err
}

func (a testApp) StartReadingMessages(shard consumer.Shard, store consumer.Store, cp pc.Checkpoint, ch chan<- consumer.EnvelopeOrError) {
	var testStore = store.(*testStore)
	testStore.coordinator = newCoordinator(shard.Context(), shard.JournalClient(),
		pf.NewExtractClient(a.workerHost.Conn))

	var err error
	if testStore.readBuilder, err = NewReadBuilder(a.service, a.journals, shard,
		func() []pf.TransformSpec { return a.transforms },
	); err != nil {
		ch <- consumer.EnvelopeOrError{Error: err}
		return
	}
	StartReadingMessages(shard.Context(), testStore.readBuilder, cp, ch)
}

func (a testApp) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	var testStore = store.(*testStore)
	return testStore.readBuilder.StartReplayRead(shard.Context(), journal, begin, end)
}

func (a testApp) NewMessage(*pb.JournalSpec) (message.Message, error) { panic("never called") }

func (a testApp) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, _ *message.Publisher) error {
	var state = store.(*testStore).State.(map[string]int)
	var msg = env.Message.(pf.IndexedShuffleResponse)

	var key = msg.Arena.Bytes(msg.PackedKey[msg.Index])
	state[string(key)]++

	return nil
}

func (a testApp) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error { return nil } // No-op.

type testMsg struct {
	Meta struct {
		UUID message.UUID `json:"uuid"`
	} `json:"_meta"`
	High int
	Low  string
}

func (m *testMsg) GetUUID() message.UUID                         { return m.Meta.UUID }
func (m *testMsg) SetUUID(uuid message.UUID)                     { m.Meta.UUID = uuid }
func (m *testMsg) NewAcknowledgement(pb.Journal) message.Message { return new(testMsg) }
