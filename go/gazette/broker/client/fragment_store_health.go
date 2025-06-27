package client

import (
	"context"

	"github.com/pkg/errors"
	pb "go.gazette.dev/core/broker/protocol"
)

// FragmentStoreHealth queries for the latest health status on the specified fragment store.
// It returns the health check response or an error if the RPC fails with a status other
// than FRAGMENT_STORE_UNHEALTHY.
func FragmentStoreHealth(ctx context.Context, client pb.JournalClient, store pb.FragmentStore) (*pb.FragmentStoreHealthResponse, error) {
	var req = pb.FragmentStoreHealthRequest{
		FragmentStore: store,
	}

	if resp, err := client.FragmentStoreHealth(pb.WithDispatchDefault(ctx), &req); err != nil {
		return nil, mapGRPCCtxErr(ctx, err)
	} else if err = resp.Validate(); err != nil {
		return nil, err
	} else if resp.Status != pb.Status_OK && resp.Status != pb.Status_FRAGMENT_STORE_UNHEALTHY {
		return nil, errors.New(resp.Status.String())
	} else {
		return resp, nil
	}
}
