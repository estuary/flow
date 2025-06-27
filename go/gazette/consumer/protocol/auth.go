package protocol

import (
	context "context"
	time "time"

	pb "go.gazette.dev/core/broker/protocol"
	grpc "google.golang.org/grpc"
)

// NewAuthShardClient returns an *AuthShardClient which uses the Authorizer
// to obtain and attach an Authorization bearer token to every issued request.
func NewAuthShardClient(sc ShardClient, auth pb.Authorizer) *AuthShardClient {
	return &AuthShardClient{Authorizer: auth, Inner: sc}
}

type AuthShardClient struct {
	Authorizer pb.Authorizer
	Inner      ShardClient
}

func (a *AuthShardClient) Stat(ctx context.Context, in *StatRequest, opts ...grpc.CallOption) (*StatResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pb.Capability_READ,
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("id", in.Shard.String()),
			},
		}
	}
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Minute); err != nil {
		return nil, err
	} else {
		return a.Inner.Stat(ctx, in, opts...)
	}
}

func (a *AuthShardClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (*ListResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pb.Capability_LIST,
			Selector:   in.Selector,
		}
	}
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Minute); err != nil {
		return nil, err
	} else {
		return a.Inner.List(ctx, in, opts...)
	}
}

func (a *AuthShardClient) Apply(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{Capability: pb.Capability_APPLY}
	}
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Minute); err != nil {
		return nil, err
	} else {
		return a.Inner.Apply(ctx, in, opts...)
	}
}

func (a *AuthShardClient) GetHints(ctx context.Context, in *GetHintsRequest, opts ...grpc.CallOption) (*GetHintsResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pb.Capability_READ,
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("id", in.Shard.String()),
			},
		}
	}
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Minute); err != nil {
		return nil, err
	} else {
		return a.Inner.GetHints(ctx, in, opts...)
	}
}

func (a *AuthShardClient) Unassign(ctx context.Context, in *UnassignRequest, opts ...grpc.CallOption) (*UnassignResponse, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{Capability: pb.Capability_APPLY}
		for _, id := range in.Shards {
			claims.Selector.Include.AddValue("id", id.String())
		}
	}
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Minute); err != nil {
		return nil, err
	} else {
		return a.Inner.Unassign(ctx, in, opts...)
	}
}

// AuthShardServer is similar to ShardServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context or Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthShardServer interface {
	Stat(context.Context, pb.Claims, *StatRequest) (*StatResponse, error)
	List(context.Context, pb.Claims, *ListRequest) (*ListResponse, error)
	Apply(context.Context, pb.Claims, *ApplyRequest) (*ApplyResponse, error)
	GetHints(context.Context, pb.Claims, *GetHintsRequest) (*GetHintsResponse, error)
	Unassign(context.Context, pb.Claims, *UnassignRequest) (*UnassignResponse, error)
}

// NewVerifiedShardServer adapts an AuthShardServer into a ShardServer by
// using the provided Verifier to verify incoming request Authorizations.
func NewVerifiedShardServer(ajs AuthShardServer, verifier pb.Verifier) *VerifiedAuthServer {
	return &VerifiedAuthServer{
		Verifier: verifier,
		Inner:    ajs,
	}
}

type VerifiedAuthServer struct {
	Verifier pb.Verifier
	Inner    AuthShardServer
}

func (s *VerifiedAuthServer) Stat(ctx context.Context, req *StatRequest) (*StatResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, pb.Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.Stat(ctx, claims, req)
	}
}

func (s *VerifiedAuthServer) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, pb.Capability_LIST); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.List(ctx, claims, req)
	}
}

func (s *VerifiedAuthServer) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, pb.Capability_APPLY); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.Apply(ctx, claims, req)
	}
}

func (s *VerifiedAuthServer) GetHints(ctx context.Context, req *GetHintsRequest) (*GetHintsResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, pb.Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.GetHints(ctx, claims, req)
	}
}

func (s *VerifiedAuthServer) Unassign(ctx context.Context, req *UnassignRequest) (*UnassignResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, pb.Capability_APPLY); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.Unassign(ctx, claims, req)
	}
}

var _ ShardServer = &VerifiedAuthServer{}
var _ ShardClient = &AuthShardClient{}
