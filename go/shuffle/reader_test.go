package shuffle

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
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

func TestStuffedMessageChannel(t *testing.T) {
	// Build a no-op ReadBuilder (no journals to walk).
	var rb = &ReadBuilder{
		shardID:  "shard",
		drainCh:  make(chan struct{}),
		journals: flow.Journals{KeySpace: new(keyspace.KeySpace)},
		members: func() []*pc.ShardSpec {
			return []*pc.ShardSpec{
				{Id: "shard", LabelSet: pb.MustLabelSet(
					labels.KeyBegin, labels.KeyBeginMin,
					labels.KeyEnd, labels.KeyEndMax,
					labels.RClockBegin, labels.RClockBeginMin,
					labels.RClockEnd, labels.RClockEndMax)},
			}
		},
	}
	var g = newGovernor(rb, pc.Checkpoint{}, flow.NewTimepoint(time.Now()))

	// Use a non-buffered channel (blocks on send), and a pre-cancelled context.
	var intoCh = make(chan consumer.EnvelopeOrError)
	var ctx, cancel = context.WithCancel(context.Background())
	cancel()

	// Expect serveDocuments immediately reads a Cancelled error.
	// It attempts to send into the channel, but can't.
	// Instead it selects over the finished context.
	g.serveDocuments(ctx, intoCh)

	var _, ok = <-intoCh
	require.False(t, ok) // Channel was closed.
}

func TestConsumerIntegration(t *testing.T) {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "a-build-id",
			Directory:  t.TempDir(),
			Source:     "file:///ab.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var derivation *pf.DerivationSpec
	require.NoError(t, catalog.Extract(args.OutputPath(), func(db *sql.DB) (err error) {
		derivation, err = catalog.LoadDerivation(db, "a/derivation")
		return err
	}))

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var builds, err = flow.NewBuildService("file://" + args.Directory + "/")
	require.NoError(t, err)
	// Fixtures which parameterize the test:
	var (
		sourcePartitions = []pb.Journal{
			"a/collection/part=10",
			"a/collection/part=20",
			"a/collection/part=42",
		}
		shards = []pc.ShardID{
			"derive/bar/abc",
			"derive/bar/def",
		}
		N = 200
	)

	// Start broker, with journal fixtures.
	var journalSpecs []*pb.JournalSpec
	for _, name := range sourcePartitions {
		journalSpecs = append(journalSpecs, brokertest.Journal(pb.JournalSpec{
			Name: name,
			LabelSet: pb.MustLabelSet(
				gazLabels.ContentType, gazLabels.ContentType_JSONLines,
				labels.Collection, "a/collection",
				labels.KeyBegin, labels.KeyBeginMin,
				labels.KeyEnd, labels.KeyEndMax,
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
			A:  ((i / 10) * 10) % 100,     // Takes values [0, 10, 20, 30, ... 90].
			AA: fmt.Sprintf("%02x", i%10), // Takes values ["00, "01", "02", "03", ... "09"].
			B:  "value",
		}

		// Half of published messages are immediately-committed.
		// Second half are transactional and require an ACK.
		var err error
		if i < N/2 {
			_, err = pub.PublishCommitted(mapping, msg)
		} else {
			_, err = pub.PublishUncommitted(mapping, msg)
		}
		require.NoError(t, err)

		// Build expected packed shuffle key.
		var k = tuple.Tuple{msg.A, msg.AA}.Pack()
		expect[hex.EncodeToString(k)]++
	}

	// Journals is a consumer-held KeySpace that observes broker-managed journals.
	journals, err := flow.NewJournalsKeySpace(ctx, etcd, "/broker.test")
	require.NoError(t, err)

	// Start consumer.
	var buildConsumer = func() *consumertest.Consumer {
		var cmr = consumertest.NewConsumer(consumertest.Args{
			C:        t,
			Etcd:     etcd,
			Journals: broker.Client(),
			App: &testApp{
				journals: journals,
				builds:   builds,
				shuffles: derivation.TaskShuffles(),
				buildID:  "a-build-id",
			},
		})
		cmr.Service.App.(*testApp).service = cmr.Service
		pf.RegisterShufflerServer(cmr.Server.GRPCServer, &API{resolve: cmr.Service.Resolver.Resolve})
		cmr.Tasks.GoRun()
		return cmr

	}
	var cmr = buildConsumer()

	// Create & install shard fixtures, each owning an equal slice of the key range.
	var shardSpecs = make([]*pc.ShardSpec, len(shards))
	for i, id := range shards {
		shardSpecs[i] = &pc.ShardSpec{
			Id:                id,
			Sources:           []pc.ShardSpec_Source{},
			RecoveryLogPrefix: "recovery/logs",
			HintPrefix:        "/hints",
			HintBackups:       1,
			MaxTxnDuration:    time.Second,
			LabelSet: labels.EncodeRange(pf.RangeSpec{
				KeyBegin:    uint32((math.MaxUint32 / len(shards)) * i),
				KeyEnd:      uint32((math.MaxUint32/len(shards))*(i+1) - 1),
				RClockBegin: 0,
				RClockEnd:   math.MaxUint32,
			}, pb.LabelSet{}),
			DisableWaitForAck: true, // Don't block waiting for an ACK that we'll deliberately delay.
		}
	}
	consumertest.CreateShards(t, cmr, shardSpecs...)

	// Block until all shards have read at least one byte from each source partition.
	for _, id := range shards {
		res, err := cmr.Service.Resolver.Resolve(consumer.ResolveArgs{
			Context: ctx,
			ShardID: id,
			ReadThrough: pb.Offsets{
				sourcePartitions[0]: 1,
				sourcePartitions[1]: 1,
				sourcePartitions[2]: 1,
			},
		})
		require.NoError(t, err)
		res.Done()
	}

	// Crash the consumer.
	cmr.Tasks.Cancel()
	require.NoError(t, cmr.Tasks.Wait())

	// Start it again, and wait for shards to recover & become primary.
	cmr = buildConsumer()
	for _, s := range shardSpecs {
		require.NoError(t, cmr.WaitForPrimary(context.Background(), s.Id, nil))
	}

	// *Now* build ACK intents. On reading the intent, the shard must go back
	// and re-play a previous portion of the journal (exercising replay mechanics).
	acks, err := pub.BuildAckIntents()
	require.NoError(t, err)

	var readThrough = make(pb.Offsets)
	for _, ack := range acks {
		var aa = ajc.StartAppend(pb.AppendRequest{Journal: ack.Journal}, nil)
		aa.Writer().Write(ack.Intent)
		require.NoError(t, aa.Release())
		require.NoError(t, aa.Err())

		// ACKs are broadcast to all readers, making them safe to read-through
		// (as an uncommitted or immediately-committed message is sent only to
		// readers to which it shuffles).
		readThrough[ack.Journal] = aa.Response().Commit.End
	}

	// Pluck out each of the worker states.
	var shardDocCounts = make([]int, len(shards))
	var mergedKeyCounts = make(map[string]int)

	for i, id := range shards {
		// Expect the shard store reflects consumed messages.
		res, err := cmr.Service.Resolver.Resolve(consumer.ResolveArgs{
			Context:     ctx,
			ShardID:     id,
			ReadThrough: readThrough,
		})
		require.Equal(t, res.Status, pc.Status_OK)
		require.NoError(t, err)

		var state = *res.Store.(*testStore).JSONFileStore.State.(*map[string]int)
		for k, c := range state {
			mergedKeyCounts[k] += c
			shardDocCounts[i] += c
		}
		res.Done() // Release resolution.
	}
	require.Equal(t, expect, mergedKeyCounts)
	// Expect that shards saw roughly equal shares of documents.
	// Values here are a regression fixture and depend on key encoding & hashing.
	require.Equal(t, []int{112, 88}, shardDocCounts)

	cmr.Tasks.Cancel()
	require.NoError(t, cmr.Tasks.Wait())

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

type testApp struct {
	service  *consumer.Service
	journals flow.Journals
	builds   *flow.BuildService
	shuffles []*pf.Shuffle
	buildID  string
}

type testStore struct {
	*consumer.JSONFileStore
	readBuilder *ReadBuilder
	coordinator *Coordinator
}

func (s *testStore) Coordinator() *Coordinator { return s.coordinator }

func (a testApp) NewStore(shard consumer.Shard, recorder *recoverylog.Recorder) (consumer.Store, error) {
	var state = make(map[string]int)
	var store, err = consumer.NewJSONFileStore(recorder, &state)
	if err != nil {
		return nil, err
	}

	readBuilder, err := NewReadBuilder(
		a.buildID,
		make(<-chan struct{}),
		a.journals,
		localPublisher,
		a.service,
		shard.Spec().Id,
		a.shuffles,
	)
	if err != nil {
		return nil, err
	}

	var coordinator = NewCoordinator(shard.Context(), a.builds, localPublisher, shard.JournalClient())

	return &testStore{
		JSONFileStore: store,
		readBuilder:   readBuilder,
		coordinator:   coordinator,
	}, err
}

func (a testApp) StartReadingMessages(shard consumer.Shard, store consumer.Store, cp pc.Checkpoint, ch chan<- consumer.EnvelopeOrError) {
	var testStore = store.(*testStore)
	var tp = flow.NewTimepoint(time.Now())
	StartReadingMessages(shard.Context(), testStore.readBuilder, cp, tp, ch)
}

func (a testApp) ReplayRange(shard consumer.Shard, store consumer.Store, journal pb.Journal, begin, end pb.Offset) message.Iterator {
	var testStore = store.(*testStore)
	return StartReplayRead(shard.Context(), testStore.readBuilder, journal, begin, end)
}

func (a testApp) ReadThrough(shard consumer.Shard, store consumer.Store, args consumer.ResolveArgs) (pb.Offsets, error) {
	var testStore = store.(*testStore)
	return testStore.readBuilder.ReadThrough(args.ReadThrough)
}

func (a testApp) NewMessage(*pb.JournalSpec) (message.Message, error) { panic("never called") }

func (a testApp) ConsumeMessage(shard consumer.Shard, store consumer.Store, env message.Envelope, _ *message.Publisher) error {
	var state = *store.(*testStore).State.(*map[string]int)
	var msg = env.Message.(pf.IndexedShuffleResponse)

	if message.GetFlags(env.GetUUID()) == message.Flag_ACK_TXN {
		return nil
	}

	var key = msg.Arena.Bytes(msg.PackedKey[msg.Index])
	state[hex.EncodeToString(key)]++

	if msg.Shuffle.GroupName != a.shuffles[0].GroupName {
		return fmt.Errorf("expected Shuffle fixture to be passed-through")
	}
	return nil
}

func (a testApp) FinalizeTxn(consumer.Shard, consumer.Store, *message.Publisher) error { return nil } // No-op.

var _ consumer.MessageProducer = (*testApp)(nil)

type testMsg struct {
	Meta struct {
		UUID message.UUID `json:"uuid"`
	} `json:"_meta"`
	A  int    `json:"a"`
	AA string `json:"aa"`
	B  string `json:"b"`
}

func (m *testMsg) GetUUID() message.UUID                         { return m.Meta.UUID }
func (m *testMsg) SetUUID(uuid message.UUID)                     { m.Meta.UUID = uuid }
func (m *testMsg) NewAcknowledgement(pb.Journal) message.Message { return new(testMsg) }
