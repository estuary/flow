package flow

import (
	"context"
	"testing"

	"github.com/estuary/flow/go/fdb/tuple"
	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

func TestPartitionPicking(t *testing.T) {
	var fixtures = buildCombineFixtures()
	var logicalPrefix, hexKey, b []byte

	var m = Mapper{
		Journals: &keyspace.KeySpace{Root: "/items"},
	}

	for ind, tc := range []struct {
		expectPrefix string
		expectKey    string
	}{
		{"/items/a/collection/bar=32/foo=A/", "27"},
		{"/items/a/collection/bar=32/foo=A/", "26"},
		{"/items/a/collection/bar=42/foo=A%2FB/", "27"},
	} {
		logicalPrefix, hexKey, b = m.logicalPrefixAndHexKey(b[:0], fixtures[ind])

		require.Equal(t, tc.expectPrefix, string(logicalPrefix))
		require.Equal(t, tc.expectKey, string(hexKey))
	}

	m.Journals.KeyValues = keyspace.KeyValues{
		{Decoded: &pb.JournalSpec{
			Name:     "a/collection/bar=32/foo=A/pivot=00",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "77"),
		}},
		{Decoded: &pb.JournalSpec{
			Name:     "a/collection/bar=32/foo=A/pivot=77",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "77", flowLabels.KeyEnd, "dd"),
		}},
		{Decoded: &pb.JournalSpec{
			Name:     "a/collection/bar=42/foo=A/pivot=00",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "dd"),
		}},
	}
	for i, j := range m.Journals.KeyValues {
		m.Journals.KeyValues[i].Raw.Key = append([]byte(m.Journals.Root+"/"), j.Decoded.(*pb.JournalSpec).Name...)
	}

	require.Equal(t,
		"a/collection/bar=32/foo=A/pivot=00",
		m.pickPartition([]byte("/items/a/collection/bar=32/foo=A/"), []byte("2345")).Name.String(),
	)
	require.Equal(t,
		"a/collection/bar=32/foo=A/pivot=77",
		m.pickPartition([]byte("/items/a/collection/bar=32/foo=A/"), []byte("90ab")).Name.String(),
	)
	require.Nil(t,
		m.pickPartition([]byte("/items/a/collection/bar=32/foo=A/"), []byte("ef01")), // Out of range.
	)
	require.Equal(t,
		"a/collection/bar=42/foo=A/pivot=00",
		m.pickPartition([]byte("/items/a/collection/bar=42/foo=A/"), []byte("abcd")).Name.String(),
	)
}

func TestBuildingUpsert(t *testing.T) {
	var fixtures = buildCombineFixtures()
	var m = Mapper{
		Journals: &keyspace.KeySpace{Root: "/items"},
	}

	require.Equal(t, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				Upsert: &pb.JournalSpec{
					Name: "a/collection/bar=32/foo=A/pivot=00",
					LabelSet: pb.MustLabelSet(
						flowLabels.Collection, "a/collection",
						labels.ContentType, labels.ContentType_JSONLines,
						flowLabels.KeyBegin, "00",
						flowLabels.KeyEnd, "ffffffff",
						flowLabels.FieldPrefix+"bar", "32",
						flowLabels.FieldPrefix+"foo", "A",
					),
					Replication: fixtures[0].Spec.JournalSpec.Replication,
					Fragment:    fixtures[0].Spec.JournalSpec.Fragment,
				},
				ExpectModRevision: 0,
			},
		},
	}, m.partitionUpsert(fixtures[0]))
}

func TestPublisherMappingIntegration(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	var ajc = client.NewAppendService(ctx, broker.Client())
	var pub = message.NewPublisher(ajc, nil)

	var journals, err = NewJournalsKeySpace(ctx, etcd, "/broker.test")
	require.NoError(t, err)
	journals.WatchApplyDelay = 0
	go journals.Watch(ctx, etcd)

	var fixtures = buildCombineFixtures()
	var mapper = &Mapper{
		Ctx:           ctx,
		JournalClient: broker.Client(),
		Journals:      journals,
	}

	for _, fixture := range fixtures {
		var _, err = pub.PublishCommitted(mapper.Map, fixture)
		require.NoError(t, err)
	}
	// Await all appends.
	for op := range ajc.PendingExcept("") {
		require.NoError(t, op.Err())
	}

	journals.Mu.RLock()
	defer journals.Mu.RUnlock()

	require.Len(t, journals.KeyValues, 2)
	for i, n := range []string{
		"a/collection/bar=32/foo=A/pivot=00",
		"a/collection/bar=42/foo=A%2FB/pivot=00",
	} {
		require.Equal(t, n, journals.KeyValues[i].Decoded.(*pb.JournalSpec).Name.String())
	}

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

func buildCombineFixtures() []Mappable {
	var spec = &pf.CollectionSpec{
		Name:            "a/collection",
		PartitionFields: []string{"bar", "foo"},
		Projections: []*pf.Projection{
			{Ptr: "/ptr"},
			{Ptr: "/ptr"},
		},
		JournalSpec: *brokertest.Journal(pb.JournalSpec{}),
	}

	return []Mappable{
		{
			Spec:       spec,
			Doc:        []byte(`{"one":1,"_uuid":"` + string(pf.DocumentUUIDPlaceholder) + `"}` + "\n"),
			PackedKey:  tuple.Tuple{true}.Pack(),
			Partitions: tuple.Tuple{32, "A"},
		},
		{
			Spec:       spec,
			Doc:        []byte(`{"two":2,"_uuid":"` + string(pf.DocumentUUIDPlaceholder) + `"}` + "\n"),
			PackedKey:  tuple.Tuple{false}.Pack(),
			Partitions: tuple.Tuple{32, "A"},
		},
		{
			Spec:       spec,
			Doc:        []byte(`{"three":3,"_uuid":"` + string(pf.DocumentUUIDPlaceholder) + `"}` + "\n"),
			PackedKey:  tuple.Tuple{true}.Pack(),
			Partitions: tuple.Tuple{42, "A/B"},
		},
	}
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
