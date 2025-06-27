package broker

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/stores"
	"go.gazette.dev/core/etcdtest"
)

func TestFragmentStoreHealthCases(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	stores.RegisterProviders(map[string]stores.Constructor{
		"s3": func(ep *url.URL) (stores.Store, error) {
			if ep.Host == "whoops" {
				return nil, fmt.Errorf("whoops")
			}
			return stores.NewMemoryStore(ep), nil
		},
	})

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})

	// Case: health of a valid FragmentStore.
	var resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "s3://bucket/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Empty(t, resp.StoreHealthError)
	require.NotNil(t, resp.Header)

	// Case: health of an unsupported FragmentStore.
	resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "s3://whoops/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_FRAGMENT_STORE_UNHEALTHY, resp.Status)
	require.Contains(t, resp.StoreHealthError, "whoops")

	// Case: context is canceled.
	ctx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately before the call

	_, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "s3://other/",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
}
