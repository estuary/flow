package shuffle

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/estuary/flow/go/flow"
	fLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/consumertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

func TestConsumerIntegration(t *testing.T) {

	// Fixtures which parameterize the test:
	var (
		sourcePartitions = []pb.Journal{
			"foo/bar=10/part=000",
			"foo/bar=10/part=001",
			"foo/bar=42/part=000",
		}
		ring = pf.Ring{
			Name:    "test-ring",
			Members: []pf.Ring_Member{{}, {}, {}},
		}
		transforms = []pf.TransformSpec{
			{
				Source: pf.TransformSpec_Source{Name: "source-foo"},
				Shuffle: pf.Shuffle{
					Transform:     "highAndLow",
					ShuffleKeyPtr: []string{"/High", "/Low"},
					BroadcastTo:   1,
				},
				Derivation: pf.TransformSpec_Derivation{Name: "derived-bar"},
			},
			{
				Source: pf.TransformSpec_Source{Name: "source-foo"},
				Shuffle: pf.Shuffle{
					Transform:     "high",
					ShuffleKeyPtr: []string{"/High"},
					BroadcastTo:   2, // Two workers read each key.
				},
				Derivation: pf.TransformSpec_Derivation{Name: "derived-bar"},
			},
			{
				Source: pf.TransformSpec_Source{Name: "source-foo"},
				Shuffle: pf.Shuffle{
					Transform:     "low",
					ShuffleKeyPtr: []string{"/Low"},
					BroadcastTo:   3, // All workers read each key.
				},
				Derivation: pf.TransformSpec_Derivation{Name: "derived-bar"},
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

	// Start broker, with journal fixtures.
	var journalSpecs []*pb.JournalSpec
	for _, name := range sourcePartitions {
		journalSpecs = append(journalSpecs, brokertest.Journal(pb.JournalSpec{
			Name: name,
			LabelSet: pb.MustLabelSet(
				labels.ContentType, labels.ContentType_JSONLines,
				fLabels.Collection, "source-foo",
			),
		}))
	}
	for index := range ring.Members {
		journalSpecs = append(journalSpecs, brokertest.Journal(pb.JournalSpec{
			Name:     pb.Journal(fmt.Sprintf("recovery/logs/%s", ring.ShardID(index).String())),
			LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
		}))
	}

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	brokertest.CreateJournals(t, broker, journalSpecs...)

	// Write data fixtures randomly across partitions.
	var ajc = client.NewAppendService(ctx, broker.Client())
	var pub = message.NewPublisher(ajc, nil)
	var mapping = func(m message.Mappable) (_ pb.Journal, contentType string, _ error) {
		return sourcePartitions[rand.Intn(len(sourcePartitions))], labels.ContentType_JSONLines, nil
	}

	var expect = testState{
		High:       make(map[uint64]int),
		Low:        make(map[string]int),
		HighAndLow: make(map[string]int),
	}
	for i := 0; i != N; i++ {
		var msg = &testMsg{
			High: ((i / 10) * 10) % 100, // Takes values [0, 10, 20, 30, ... 90].
			Low:  strconv.Itoa(i % 10),  // Takes values ["0, "1", "2", "3", ... "9"].
		}
		var aa, _ = pub.PublishCommitted(mapping, msg)
		require.NoError(t, aa.Err())

		expect.HighAndLow[fmt.Sprintf("%d,%s", msg.High, msg.Low)]++
		expect.High[uint64(msg.High)] += 2 // Broadcast to 2 shards.
		expect.Low[msg.Low] += 3           // Broadcast to 3 shards.
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
			workerHost: wh,
			ring:       ring,
			transforms: transforms,
		},
	})
	c.Service.App.(*testApp).service = c.Service
	pf.RegisterShufflerServer(c.Server.GRPCServer, &API{resolve: c.Service.Resolver.Resolve})
	c.Tasks.GoRun()

	var shardSpecs []*pc.ShardSpec
	for index := range ring.Members {
		shardSpecs = append(shardSpecs, &pc.ShardSpec{
			Id:                ring.ShardID(index),
			Sources:           []pc.ShardSpec_Source{},
			RecoveryLogPrefix: "recovery/logs",
			HintPrefix:        "/hints",
			HintBackups:       1,
			MaxTxnDuration:    time.Second,
			LabelSet: pb.MustLabelSet(
				fLabels.Derivation, "derived-bar",
				fLabels.WorkerIndex, strconv.Itoa(index)),
		})
	}
	consumertest.CreateShards(t, c, shardSpecs...)

	// TODO(johnny): Wait for consumers more elegantly & correctly than this.
	time.Sleep(time.Second)

	// TODO(johnny): We should have some coverage of journal replays.
	// Skipping for now, as it's kind of a "it works or it doesn't"
	// feature which future tests are likely to cover.

	// Pluck out each of the worker states.
	var merged = testState{
		High:       make(map[uint64]int),
		Low:        make(map[string]int),
		HighAndLow: make(map[string]int),
	}

	for index := range ring.Members {
		// Expect the shard store reflects consumed messages.
		res, err := c.Service.Resolver.Resolve(consumer.ResolveArgs{Context: ctx, ShardID: ring.ShardID(index)})
		require.NoError(t, err)

		var state = res.Store.(*testStore).JSONFileStore.State.(*testState)
		for h, c := range state.High {
			merged.High[h] += c
		}
		for l, c := range state.Low {
			merged.Low[l] += c
		}
		for hl, c := range state.HighAndLow {
			merged.HighAndLow[hl] += c
		}
		res.Done() // Release resolution.
	}
	require.Equal(t, expect, merged)
}

type testApp struct {
	service    *consumer.Service
	workerHost *flow.WorkerHost
	ring       pf.Ring
	transforms []pf.TransformSpec
}

type testStore struct {
	*consumer.JSONFileStore
	readBuilder *ReadBuilder
	coordinator *coordinator
}

type testState struct {
	High       map[uint64]int
	Low        map[string]int
	HighAndLow map[string]int
}

func (s *testStore) Coordinator() *coordinator { return s.coordinator }

func (a testApp) NewStore(shard consumer.Shard, recorder *recoverylog.Recorder) (consumer.Store, error) {
	var store, err = consumer.NewJSONFileStore(recorder, &testState{
		High:       make(map[uint64]int),
		Low:        make(map[string]int),
		HighAndLow: make(map[string]int),
	})
	return &testStore{JSONFileStore: store}, err
}

func (a testApp) StartReadingMessages(shard consumer.Shard, store consumer.Store, cp pc.Checkpoint, ch chan<- consumer.EnvelopeOrError) {
	var testStore = store.(*testStore)
	testStore.coordinator = newCoordinator(shard.Context(), shard.JournalClient(),
		pf.NewExtractClient(a.workerHost.Conn))

	var err error
	if testStore.readBuilder, err = NewReadBuilder(a.service, shard,
		func() pf.Ring { return a.ring },
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

func (a testApp) ConsumeMessage(_ consumer.Shard, store consumer.Store, env message.Envelope, _ *message.Publisher) error {
	var state = store.(*testStore).State.(*testState)
	var msg = env.Message.(pf.IndexedShuffleResponse)

	// Update counter of observations of each shuffle key,
	// switched on the transform which read this message.
	switch msg.Transform {
	case "high":
		var f = msg.ShuffleKey[0].Values[msg.Index]
		if f.Kind != pf.Field_Value_UNSIGNED {
			panic(f)
		}
		state.High[f.Unsigned] = state.High[f.Unsigned] + 1
	case "low":
		var f = msg.ShuffleKey[0].Values[msg.Index]
		if f.Kind != pf.Field_Value_STRING {
			panic(f)
		}
		state.Low[string(msg.Arena.Bytes(f.Bytes))] =
			state.Low[string(msg.Arena.Bytes(f.Bytes))] + 1
	case "highAndLow":
		var fh = msg.ShuffleKey[0].Values[msg.Index]
		var fl = msg.ShuffleKey[1].Values[msg.Index]
		if fh.Kind != pf.Field_Value_UNSIGNED {
			panic(fh)
		}
		if fl.Kind != pf.Field_Value_STRING {
			panic(fl)
		}
		var f = fmt.Sprintf("%d,%s", fh.Unsigned, msg.Arena.Bytes(fl.Bytes))
		state.HighAndLow[f] = state.HighAndLow[f] + 1

	default:
		panic("unknown transform: " + msg.Transform.String())
	}

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
