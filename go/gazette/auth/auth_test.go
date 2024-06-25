package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/auth"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/metadata"
)

func TestKeyedAuthCases(t *testing.T) {
	ka1, err := auth.NewKeyedAuth("c2VjcmV0,b3RoZXI=")
	require.NoError(t, err)
	ka2, err := auth.NewKeyedAuth("b3RoZXI=,c2VjcmV0")
	require.NoError(t, err)
	kaM, err := auth.NewKeyedAuth("YXNkZg==,AA==")
	require.NoError(t, err)

	// Authorize with one KeyedAuth...
	ctx, err := ka1.Authorize(context.Background(),
		pb.Claims{
			Capability: pb.Capability_APPEND | pb.Capability_LIST,
			Selector:   pb.MustLabelSelector("hi=there"),
		}, time.Hour)
	require.NoError(t, err)

	var md, _ = metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(ctx, md)

	// ...and verify with the other.
	_, cancel, claims, err := ka2.Verify(ctx, pb.Capability_APPEND)
	require.NoError(t, err)
	cancel()
	require.Equal(t, pb.MustLabelSelector("hi=there"), claims.Selector)

	// Unless the capability doesn't match.
	_, _, _, err = ka2.Verify(ctx, pb.Capability_REPLICATE)
	require.EqualError(t, err,
		"rpc error: code = Unauthenticated desc = authorization is missing required REPLICATE capability")

	// A KeyedAuth with a diferent key rejects it.
	_, _, _, err = kaM.Verify(ctx, pb.Capability_APPEND)
	require.EqualError(t, err,
		"rpc error: code = Unauthenticated desc = verifying Authorization: token signature is invalid: signature is invalid")

	// A KeyedAuth that allows pass-through will accept a request without a token.
	_, cancel, claims, err = kaM.Verify(context.Background(), pb.Capability_READ)
	require.NoError(t, err)
	cancel()
	require.Equal(t, pb.MustLabelSelector(""), claims.Selector)
}
