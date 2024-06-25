package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/etcdtest"
)

func TestFragmentsResolutionCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	var fragments = buildFragmentsFixture()

	// Case: Resolution error.
	var resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal: "a/missing/journal",
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status: pb.Status_JOURNAL_NOT_FOUND,
		Header: *broker.header("a/missing/journal"),
	}, resp)

	// Case: Read from a write only journal.
	setTestJournal(broker, pb.JournalSpec{Name: "write/only", Replication: 1, Flags: pb.JournalSpec_O_WRONLY}, broker.id)
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal: "write/only",
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status: pb.Status_NOT_ALLOWED,
		Header: *broker.header("write/only"),
	}, resp)

	// Case: Proxy request to peer.
	var peer = newMockBroker(t, etcd, pb.ProcessSpec_ID{Zone: "peer", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "proxy/journal", Replication: 1}, peer.id)
	var proxyHeader = broker.header("proxy/journal")

	peer.ListFragmentsFunc = func(ctx context.Context, req *pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
		require.Equal(t, &pb.FragmentsRequest{
			Header:        proxyHeader,
			Journal:       "proxy/journal",
			BeginModTime:  time.Unix(0, 0).Unix(),
			EndModTime:    time.Unix(0, 0).Unix(),
			NextPageToken: 0,
			PageLimit:     defaultPageLimit,
			DoNotProxy:    false,
		}, req)
		return &pb.FragmentsResponse{
			Status:        pb.Status_OK,
			Header:        *proxyHeader,
			Fragments:     fragments,
			NextPageToken: fragments[5].Spec.End,
		}, nil
	}

	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{Journal: "proxy/journal"})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        *proxyHeader,
		Fragments:     fragments,
		NextPageToken: fragments[5].Spec.End,
	}, resp)

	// Case: Insufficient claimed capability.
	_, err = broker.client().ListFragments(
		pb.WithClaims(ctx, pb.Claims{Capability: pb.Capability_APPEND}),
		&pb.FragmentsRequest{Journal: "proxy/journal"})
	require.ErrorContains(t, err, "authorization is missing required READ capability")

	// Case: Insufficient claimed selector.
	resp, err = broker.client().ListFragments(
		pb.WithClaims(ctx,
			pb.Claims{
				Capability: pb.Capability_READ,
				Selector:   pb.MustLabelSelector("name=something/else"),
			}),
		&pb.FragmentsRequest{Journal: "proxy/journal"})
	require.NoError(t, err)
	require.Equal(t, pb.Status_JOURNAL_NOT_FOUND, resp.Status) // Journal not visible to these claims.
	require.Len(t, resp.Header.Route.Endpoints, 0)

	broker.cleanup()
}

func TestFragmentsListCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	setTestJournal(broker, pb.JournalSpec{Name: "a/journal", Replication: 1}, broker.id)

	var fragments = buildFragmentsFixture()
	var oneSec = time.Second
	var expectHeader = *broker.header("a/journal")

	// Case: Request validation error.
	var _, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		BeginModTime:  50,
		EndModTime:    40,
		NextPageToken: 0,
		DoNotProxy:    false,
	})
	require.EqualError(t, err, `rpc error: code = Unknown desc = invalid EndModTime (40 must be after 50)`)

	// Case: Fetch fragments with unbounded time range.
	// Asynchronously seed the fragment index with fixture data.
	time.AfterFunc(time.Millisecond, func() {
		broker.replica("a/journal").index.ReplaceRemote(buildFragmentSet(fragments))
	})
	resp, err := broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		SignatureTTL: &oneSec,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		Fragments:     fragments,
		NextPageToken: 0,
	}, resp)

	// Case: Fetch fragments with bounded time range
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		BeginModTime: 100,
		EndModTime:   180,
		SignatureTTL: &oneSec,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status: pb.Status_OK,
		Header: *broker.header("a/journal"),
		Fragments: []pb.FragmentsResponse__Fragment{
			fragments[1],
			fragments[3],
		},
		NextPageToken: 0,
	}, resp)

	// Case: Fetch fragments with unbounded EndModTime
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		BeginModTime: 100,
		SignatureTTL: &oneSec,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		Fragments:     fragments[1:],
		NextPageToken: 0,
	}, resp)

	// Case: Fetch paginated fragments
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		PageLimit:    3,
		SignatureTTL: &oneSec,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		Fragments:     fragments[:3],
		NextPageToken: fragments[3].Spec.Begin,
	}, resp)
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		PageLimit:     3,
		SignatureTTL:  &oneSec,
		NextPageToken: resp.NextPageToken,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		Fragments:     fragments[3:],
		NextPageToken: 0,
	}, resp)

	// Case: Fetch with a NextPageToken which does not correspond to a
	// Begin in the fragment set.
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		SignatureTTL:  &oneSec,
		NextPageToken: 120,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		Fragments:     fragments[3:],
		NextPageToken: 0,
	}, resp)

	// Case: Fetch fragments outside of time range.
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:      "a/journal",
		BeginModTime: 10000,
		EndModTime:   20000,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		NextPageToken: 0,
	}, resp)

	// Case: Fetch fragments with a NextPageToken larger than max fragment offset.
	resp, err = broker.client().ListFragments(ctx, &pb.FragmentsRequest{
		Journal:       "a/journal",
		NextPageToken: 1000,
	})
	require.NoError(t, err)
	require.Equal(t, &pb.FragmentsResponse{
		Status:        pb.Status_OK,
		Header:        expectHeader,
		NextPageToken: 0,
	}, resp)

	broker.cleanup()
}

// return a fixture which can be modified as needed over the course of a test.
var buildFragmentsFixture = func() []pb.FragmentsResponse__Fragment {
	return []pb.FragmentsResponse__Fragment{
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            0,
				End:              40,
				ModTime:          90,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000000-0000000000000028-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            40,
				End:              110,
				ModTime:          101,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000028-000000000000006e-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            99,
				End:              130,
				ModTime:          200,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000063-0000000000000082-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            131,
				End:              318,
				ModTime:          150,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/0000000000000083-000000000000013e-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal:          "a/journal",
				Begin:            319,
				End:              400,
				ModTime:          290,
				BackingStore:     pb.FragmentStore("file:///root/one/"),
				CompressionCodec: pb.CompressionCodec_NONE,
			},
			SignedUrl: "file:///root/one/a/journal/000000000000013f-0000000000000190-0000000000000000000000000000000000000000.raw",
		},
		{
			Spec: pb.Fragment{
				Journal: "a/journal",
				Begin:   380,
				End:     600,
			},
		},
	}
}

func buildFragmentSet(fragments []pb.FragmentsResponse__Fragment) fragment.CoverSet {
	var set = fragment.CoverSet{}
	for _, f := range fragments {
		set, _ = set.Add(fragment.Fragment{Fragment: f.Spec})
	}
	return set
}
