package flow

import (
	"context"
	"testing"

	flowLabels "github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
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
	var cr = buildMapperCombineResponseFixture()
	var logicalPrefix, hexKey, b []byte

	var m = Mapping{
		journals:   &keyspace.KeySpace{Root: "/items"},
		collection: "a/collection",
		partitions: []string{"bar", "fo o"},
	}

	for ind, tc := range []struct {
		expectPrefix string
		expectKey    string
	}{
		{"/items/a/collection/bar=32/fo%20o=A/", "0a"},
		{"/items/a/collection/bar=32/fo%20o=A/", "0b"},
		{"/items/a/collection/bar=42/fo%20o=A%2FB/", "0a"},
	} {
		logicalPrefix, hexKey, b = m.logicalPrefixAndHexKey(b[:0],
			pf.IndexedCombineResponse{CombineResponse: cr, Index: ind})

		require.Equal(t, tc.expectPrefix, string(logicalPrefix))
		require.Equal(t, tc.expectKey, string(hexKey))
	}

	m.journals.KeyValues = keyspace.KeyValues{
		{Decoded: &pb.JournalSpec{
			Name:     "a/collection/bar=32/fo%20o=A/_phys=0",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "77"),
		}},
		{Decoded: &pb.JournalSpec{
			Name:     "a/collection/bar=32/fo%20o=A/_phys=1",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "77", flowLabels.KeyEnd, "dd"),
		}},
		{Decoded: &pb.JournalSpec{
			Name:     "a/collection/bar=42/fo%20o=A/_phys=0",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "dd"),
		}},
	}
	for i, j := range m.journals.KeyValues {
		m.journals.KeyValues[i].Raw.Key = append([]byte(m.journals.Root+"/"), j.Decoded.(*pb.JournalSpec).Name...)
	}

	require.Equal(t,
		"a/collection/bar=32/fo%20o=A/_phys=0",
		m.pickPartition([]byte("/items/a/collection/bar=32/fo%20o=A/"), []byte("2345")).Name.String(),
	)
	require.Equal(t,
		"a/collection/bar=32/fo%20o=A/_phys=1",
		m.pickPartition([]byte("/items/a/collection/bar=32/fo%20o=A/"), []byte("90ab")).Name.String(),
	)
	require.Nil(t,
		m.pickPartition([]byte("/items/a/collection/bar=32/fo%20o=A/"), []byte("ef01")), // Out of range.
	)
	require.Equal(t,
		"a/collection/bar=42/fo%20o=A/_phys=0",
		m.pickPartition([]byte("/items/a/collection/bar=42/fo%20o=A/"), []byte("abcd")).Name.String(),
	)
}

func TestBuildingUpsert(t *testing.T) {
	var m = Mapping{
		journals:   &keyspace.KeySpace{Root: "/items"},
		collection: "a/collection",
		partitions: []string{"bar", "fo o"},
		model:      *brokertest.Journal(pb.JournalSpec{}),
	}
	var cr = buildMapperCombineResponseFixture()

	require.Equal(t, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				Upsert: &pb.JournalSpec{
					Name: "a/collection/bar=32/fo%20o=A/_phys=0000",
					LabelSet: pb.MustLabelSet(
						flowLabels.Collection, "a/collection",
						labels.ContentType, labels.ContentType_JSONLines,
						flowLabels.KeyBegin, "",
						flowLabels.KeyEnd, "ffffffff",
						flowLabels.FieldPrefix+"bar", "32",
						flowLabels.FieldPrefix+"fo%20o", "A",
					),
					Replication: m.model.Replication,
					Fragment:    m.model.Fragment,
				},
				ExpectModRevision: 0,
			},
		},
	}, m.partitionUpsert(pf.IndexedCombineResponse{CombineResponse: cr, Index: 0}))
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

	var mapper = &Mapping{
		ctx:        ctx,
		rjc:        broker.Client(),
		collection: "a/collection",
		partitions: []string{"bar", "fo o"},
		model:      *brokertest.Journal(pb.JournalSpec{}),
		journals:   journals,
	}

	var cr = buildMapperCombineResponseFixture()
	for ind := range cr.DocsJson {
		var _, err = pub.PublishCommitted(mapper.Map, pf.IndexedCombineResponse{
			CombineResponse: cr,
			Index:           ind,
		})
		require.NoError(t, err)
	}
	// Await all appends.
	for op := range ajc.PendingExcept("") {
		require.NoError(t, op.Err())
	}

	journals.Mu.RLock()
	defer journals.Mu.RUnlock()

	require.Len(t, journals.KeyValues, 2)
	require.Equal(t,
		"a/collection/bar=32/fo%20o=A/_phys=0000",
		journals.KeyValues[0].Decoded.(*pb.JournalSpec).Name.String())
	require.Equal(t,
		"a/collection/bar=32/fo%20o=A/_phys=0000",
		journals.KeyValues[0].Decoded.(*pb.JournalSpec).Name.String())

	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

func buildMapperCombineResponseFixture() *pf.CombineResponse {
	var cr = new(pf.CombineResponse)

	cr.DocsJson = cr.Arena.AddAll(
		[]byte(`{"one":1,"_uuid":"`+string(pf.DocumentUUIDPlaceholder)+`"}`+"\n"),
		[]byte(`{"two":2,"_uuid":"`+string(pf.DocumentUUIDPlaceholder)+`"}`+"\n"),
		[]byte(`{"three":3,"_uuid":"`+string(pf.DocumentUUIDPlaceholder)+`"}`+"\n"),
	)
	cr.Fields = []pf.Field{
		// Logical partition portion of fields.
		{
			Values: []pf.Field_Value{
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 32},
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 32},
				{Kind: pf.Field_Value_UNSIGNED, Unsigned: 42},
			},
		},
		{
			Values: []pf.Field_Value{
				{Kind: pf.Field_Value_STRING, Bytes: cr.Arena.Add([]byte("A"))},
				{Kind: pf.Field_Value_STRING, Bytes: cr.Arena.Add([]byte("A"))},
				{Kind: pf.Field_Value_STRING, Bytes: cr.Arena.Add([]byte("A/B"))},
			},
		},
		// Collection key portion of fields.
		{
			Values: []pf.Field_Value{
				{Kind: pf.Field_Value_TRUE},
				{Kind: pf.Field_Value_FALSE},
				{Kind: pf.Field_Value_TRUE},
			},
		},
	}
	return cr
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
