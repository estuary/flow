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
	defer broker.cleanup()

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

func TestFragmentStoreHealthDeleteProbe(t *testing.T) {
	var ctx, etcd = pb.WithDispatchDefault(context.Background()), etcdtest.TestClient()
	defer etcdtest.Cleanup()

	// The "no-delete" store denies delete but delegates all else to memory, so
	// the periodic health check passes and the RPC reaches the delete probe.
	stores.RegisterProviders(map[string]stores.Constructor{
		"s3": func(ep *url.URL) (stores.Store, error) {
			var mem = stores.NewMemoryStore(ep)
			if ep.Host == "no-delete" {
				return &stores.CallbackStore{
					Fallback: mem,
					RemoveFunc: func(_ stores.Store, ctx context.Context, _ string) error {
						if _, ok := ctx.Deadline(); !ok {
							return fmt.Errorf("delete probe ran without a deadline")
						}
						return fmt.Errorf("simulated missing delete permission")
					},
				}, nil
			}
			return mem, nil
		},
	})

	var broker = newTestBroker(t, etcd, pb.ProcessSpec_ID{Zone: "local", Suffix: "broker"})
	defer broker.cleanup()

	// Healthy store grants delete: passes under a sub-prefix.
	var resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore:     "s3://bucket/",
		CheckDelete:       true,
		CheckDeletePrefix: "recovery/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Empty(t, resp.StoreHealthError)

	// Store lacks delete permission: probe fails.
	resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore:     "s3://no-delete/",
		CheckDelete:       true,
		CheckDeletePrefix: "recovery/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_FRAGMENT_STORE_UNHEALTHY, resp.Status)
	require.Contains(t, resp.StoreHealthError, "delete-probe DELETE failed")
	require.Contains(t, resp.StoreHealthError, "simulated missing delete permission")

	// check_delete false: reports healthy (background-check behavior).
	resp, err = broker.client().FragmentStoreHealth(ctx, &pb.FragmentStoreHealthRequest{
		FragmentStore: "s3://no-delete/",
	})
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Empty(t, resp.StoreHealthError)
}
