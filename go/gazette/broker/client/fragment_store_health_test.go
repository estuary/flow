package client

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
)

func TestCheckFragmentStoreHealth(t *testing.T) {
	var broker = teststub.NewBroker(t)
	defer broker.Cleanup()

	var ctx = context.Background()

	// Case 1: Successful health check with healthy store.
	broker.FragmentStoreHealthFunc = func(ctx context.Context, req *pb.FragmentStoreHealthRequest) (*pb.FragmentStoreHealthResponse, error) {
		return &pb.FragmentStoreHealthResponse{
			Status: pb.Status_OK,
			Header: *buildHeaderFixture(broker),
		}, nil
	}

	resp, err := FragmentStoreHealth(ctx, broker.Client(), "s3://my-bucket/")
	require.NoError(t, err)
	require.Equal(t, pb.Status_OK, resp.Status)
	require.Empty(t, resp.StoreHealthError)

	// Case 2: Successful health check with unhealthy store.
	broker.FragmentStoreHealthFunc = func(ctx context.Context, req *pb.FragmentStoreHealthRequest) (*pb.FragmentStoreHealthResponse, error) {
		return &pb.FragmentStoreHealthResponse{
			Status:           pb.Status_FRAGMENT_STORE_UNHEALTHY,
			StoreHealthError: "store is unhealthy: connection timeout",
			Header:           *buildHeaderFixture(broker),
		}, nil
	}

	resp, err = FragmentStoreHealth(ctx, broker.Client(), "s3://my-bucket/")
	require.NoError(t, err)
	require.Equal(t, pb.Status_FRAGMENT_STORE_UNHEALTHY, resp.Status)
	require.Equal(t, "store is unhealthy: connection timeout", resp.StoreHealthError)

	// Case 3: RPC error.
	broker.FragmentStoreHealthFunc = func(ctx context.Context, req *pb.FragmentStoreHealthRequest) (*pb.FragmentStoreHealthResponse, error) {
		return nil, errors.New("network error")
	}

	resp, err = FragmentStoreHealth(ctx, broker.Client(), "s3://my-bucket/")
	require.Error(t, err)
	require.Contains(t, err.Error(), "network error")
	require.Nil(t, resp)

	// Case 4: Unexpected status.
	broker.FragmentStoreHealthFunc = func(ctx context.Context, req *pb.FragmentStoreHealthRequest) (*pb.FragmentStoreHealthResponse, error) {
		return &pb.FragmentStoreHealthResponse{
			Status: pb.Status_JOURNAL_NOT_FOUND,
			Header: *buildHeaderFixture(broker),
		}, nil
	}

	resp, err = FragmentStoreHealth(ctx, broker.Client(), "s3://my-bucket/")
	require.Error(t, err)
	require.Equal(t, "JOURNAL_NOT_FOUND", err.Error())
	require.Nil(t, resp)
}
