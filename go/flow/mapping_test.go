package flow

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"testing"

	"github.com/estuary/flow/go/bindings"
	flowLabels "github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	"go.gazette.dev/core/etcdtest"
	"go.gazette.dev/core/message"
)

func TestPartitionPicking(t *testing.T) {
	var fixtures = buildCombineFixtures(t)
	var logicalPrefix, hexKey, b []byte

	for ind, tc := range []struct {
		expectPrefix string
		expectKey    string
	}{
		{"a/collection/ffffffffffffffff/bar=%_32/foo=A/", "b9f08d38"},
		{"a/collection/ffffffffffffffff/bar=%_32/foo=A/", "1505e3cb"},
		{"a/collection/ffffffffffffffff/bar=%_42/foo=A%2FB/", "b9f08d38"},
	} {
		logicalPrefix, hexKey, b = logicalPrefixAndHexKey(b[:0], fixtures[ind])

		require.Equal(t, tc.expectPrefix, string(logicalPrefix))
		require.Equal(t, tc.expectKey, string(hexKey))
	}

	var journals = []pb.ListResponse_Journal{
		{Spec: pb.JournalSpec{
			Name:     "a/collection/bar=32/foo=A/pivot=00",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "77"),
		}},
		{Spec: pb.JournalSpec{
			Name:     "a/collection/bar=32/foo=A/pivot=77",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "78", flowLabels.KeyEnd, "dd"),
		}},
		{Spec: pb.JournalSpec{
			Name:     "a/collection/bar=42/foo=A/pivot=00",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "dd"),
		}},
		{Spec: pb.JournalSpec{
			Name:     "b/collection/qib=abcabcabcabcabcabcabc/pivot=00",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "dd"),
		}},
		{Spec: pb.JournalSpec{
			Name:     "b/collection/qib=d/pivot=00",
			LabelSet: pb.MustLabelSet(flowLabels.KeyBegin, "00", flowLabels.KeyEnd, "dd"),
		}},
	}

	require.Equal(t,
		"a/collection/bar=32/foo=A/pivot=00",
		pickPartition([]byte("a/collection/bar=32/foo=A/"), []byte("23"), journals).Name.String(),
	)
	require.Equal(t,
		"a/collection/bar=32/foo=A/pivot=77",
		pickPartition([]byte("a/collection/bar=32/foo=A/"), []byte("90"), journals).Name.String(),
	)
	require.Nil(t,
		pickPartition([]byte("a/collection/bar=32/foo=A/"), []byte("ef"), journals), // Out of range.
	)
	require.Equal(t,
		"a/collection/bar=42/foo=A/pivot=00",
		pickPartition([]byte("a/collection/bar=42/foo=A/"), []byte("ab"), journals).Name.String(),
	)
	require.Equal(t,
		"a/collection/bar=32/foo=A/pivot=00",
		pickPartition([]byte("a/collection/bar=32/foo=A/"), []byte("77"), journals).Name.String(),
	)
	require.Equal(t,
		"a/collection/bar=42/foo=A/pivot=00",
		pickPartition([]byte("a/collection/bar=42/foo=A/"), []byte("dd"), journals).Name.String(),
	)
	require.Nil(t,
		pickPartition([]byte("a/collection/bar=52/foo=A/"), []byte("00"), journals),
	)
	require.Nil(t,
		pickPartition([]byte("b/collection/qib=ab/"), []byte("00"), journals),
	)
	require.Equal(t,
		"b/collection/qib=abcabcabcabcabcabcabc/pivot=00",
		pickPartition([]byte("b/collection/qib=abcabcabcabcabcabcabc/"), []byte("00"), journals).Name.String(),
	)
	require.Equal(t,
		"b/collection/qib=d/pivot=00",
		pickPartition([]byte("b/collection/qib=d/"), []byte("dc"), journals).Name.String(),
	)

}

func TestAppendHexEncoding(t *testing.T) {
	// Expect appendHex32 produces identical results to
	// labels.EncodeRange for a variety of padding edge cases.
	var cases = []uint32{
		0x00000000,
		0x00000001,
		0x00000020,
		0x00000300,
		0x00004000,
		0x00050000,
		0x00600000,
		0x07000000,
		0x80000000,
		0x87654321,
		0xffffffff,
	}
	for _, tc := range cases {
		var b = appendHex32([]byte("foo"), tc)[3:]
		require.Equal(t, fmt.Sprintf("%08x", tc), string(b), tc)
	}
}

func TestPublisherMappingIntegration(t *testing.T) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var broker = brokertest.NewBroker(t, etcd, "local", "broker")
	var ajc = client.NewAppendService(ctx, broker.Client())
	var pub = message.NewPublisher(ajc, nil)

	var fixtures = buildCombineFixtures(t)
	var mapper = NewMapper(ctx, broker.Client())

	var list = client.NewWatchedList(ctx, broker.Client(), pb.ListRequest{}, nil)
	require.NoError(t, <-list.UpdateCh())

	// Apply one of the fixture partitions out-of-band. The Mapper initially
	// will not see this partition, will attempt to create it, and will then
	// conflict. We expect that it gracefully handles this conflict.
	applySpec, err := BuildPartitionSpec(fixtures[0].Spec.PartitionTemplate,
		flowLabels.EncodePartitionLabels(
			fixtures[0].Spec.PartitionFields, fixtures[0].Partitions,
			pb.MustLabelSet(
				flowLabels.KeyBegin, flowLabels.KeyBeginMin,
				flowLabels.KeyEnd, flowLabels.KeyEndMax,
			)))
	require.NoError(t, err)

	_, err = client.ApplyJournals(ctx, ajc, &pb.ApplyRequest{
		Changes: []pb.ApplyRequest_Change{
			{
				Upsert:            applySpec,
				ExpectModRevision: 0,
			},
		},
	})
	require.NoError(t, err)

	// Publish all fixtures, causing the Mapper to create partitions as required.
	for _, fixture := range fixtures {
		fixture.List = list
		var _, err = pub.PublishCommitted(mapper.Map, fixture)
		require.NoError(t, err)
	}
	// Await all appends.
	for op := range ajc.PendingExcept("") {
		require.NoError(t, op.Err())
	}

	require.Len(t, list.List().Journals, 2)
	for i, n := range []string{
		"a/collection/ffffffffffffffff/bar=%_32/foo=A/pivot=00",
		"a/collection/ffffffffffffffff/bar=%_42/foo=A%2FB/pivot=00",
	} {
		require.Equal(t, n, list.List().Journals[i].Spec.Name.String())
	}

	cancel()
	broker.Tasks.Cancel()
	require.NoError(t, broker.Tasks.Wait())
}

func buildCombineFixtures(t *testing.T) []Mappable {
	var args = bindings.BuildArgs{
		Context:  context.Background(),
		FileRoot: "./testdata",
		BuildAPI_Config: pf.BuildAPI_Config{
			BuildId:    "4444444444444444",
			BuildDb:    path.Join(t.TempDir(), "build.db"),
			Source:     "file:///mapping_test.flow.yaml",
			SourceType: pf.ContentType_CATALOG,
		}}
	require.NoError(t, bindings.BuildCatalog(args))

	var spec *pf.CollectionSpec
	require.NoError(t, catalog.Extract(args.BuildDb, func(db *sql.DB) (err error) {
		spec, err = catalog.LoadCollection(db, "a/collection")
		return err
	}))
	// Tweak spec for use with in-process broker.
	spec.PartitionTemplate.Replication = 1
	spec.PartitionTemplate.Fragment.Stores = nil

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

func TestHighwayHashRegression(t *testing.T) {
	var cases = []struct {
		expect uint32
		given  tuple.Tuple
	}{
		// Expect that small (e.x. single bit) changes to the input wildly change the output.
		{0xb9f08d38, tuple.Tuple{true}},
		{0x1505e3cb, tuple.Tuple{false}},
		{0x6ae719f3, tuple.Tuple{"foo", "bar"}},
		{0x8adddd61, tuple.Tuple{"foobar"}},
		{0x7273e587, tuple.Tuple{"foobas"}},
		{0xf4ec4d33, tuple.Tuple{"1"}},
		{0x1e023d95, tuple.Tuple{"2"}},
		{0x38a34efe, tuple.Tuple{"3"}},
		{0x17751bae, tuple.Tuple{"10"}},
		{0x87d93806, tuple.Tuple{"11"}},
		{0x3c90c1d9, tuple.Tuple{1}},
		{0x97901bac, tuple.Tuple{2}},
		{0xcbc7f1e2, tuple.Tuple{3}},
		{0xd1d3f3eb, tuple.Tuple{10}},
	}
	for _, tc := range cases {
		require.Equal(t, tc.expect, PackedKeyHash_HH64(tc.given.Pack()))
	}
}

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
