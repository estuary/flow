package runtime

import (
	"context"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

// NewAuthShufflerClient returns a ShufflerClient which uses the Authorizer
// to obtain and attach an Authorization bearer token to every issued request.
func NewAuthShufflerClient(sc ShufflerClient, auth pb.Authorizer) ShufflerClient {
	return &authShufflerClient{auth: auth, sc: sc}
}

type authShufflerClient struct {
	auth pb.Authorizer
	sc   ShufflerClient
}

func (a *authShufflerClient) Shuffle(ctx context.Context, in *ShuffleRequest, opts ...grpc.CallOption) (Shuffler_ShuffleClient, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		claims = pb.Claims{
			Capability: pf.Capability_SHUFFLE,
			Selector: pb.LabelSelector{
				Include: pb.MustLabelSet("id", in.Coordinator.String()),
			},
		}
	}
	if ctx, err := a.auth.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.sc.Shuffle(ctx, in, opts...)
	}
}

// AuthShufflerServer is similar to ShufflerServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context or Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthShufflerServer interface {
	Shuffle(pb.Claims, *ShuffleRequest, Shuffler_ShuffleServer) error
}

// NewVerifiedShufflerServer adapts an AuthShufflerServer into a ShufflerServer by
// using the provided Verifier to verify incoming request Authorizations.
func NewVerifiedShufflerServer(ss AuthShufflerServer, verifier pb.Verifier) ShufflerServer {
	return &verifiedShufflerServer{
		inner:    ss,
		verifier: verifier,
	}
}

type verifiedShufflerServer struct {
	inner    AuthShufflerServer
	verifier pb.Verifier
}

func (a *verifiedShufflerServer) Shuffle(in *ShuffleRequest, stream Shuffler_ShuffleServer) error {
	if ctx, cancel, claims, err := a.verifier.Verify(stream.Context(), pf.Capability_SHUFFLE); err != nil {
		return err
	} else {
		defer cancel()
		return a.inner.Shuffle(claims, in, verifiedShuffleStream{ctx, stream})
	}
}

var _ ShufflerServer = &verifiedShufflerServer{}
var _ ShufflerClient = &authShufflerClient{}

type verifiedShuffleStream struct {
	ctx context.Context
	Shuffler_ShuffleServer
}

func (s verifiedShuffleStream) Context() context.Context { return s.ctx }

// AuthTaskControlServer is similar to TaskControlServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthTaskControlServer interface {
	SyncNow(pb.Claims, *SyncNowRequest, TaskControl_SyncNowServer) error
}

// NewVerifiedTaskControlServer adapts an AuthTaskControlServer into a
// TaskControlServer by using the provided Verifier to verify incoming request
// Authorizations. TaskControl's AuthN floor is gazette READ: the fine-grain
// AuthZ check — that the claims' label selector covers the task's shards —
// is deferred to shard resolution.
func NewVerifiedTaskControlServer(tc AuthTaskControlServer, verifier pb.Verifier) TaskControlServer {
	return &verifiedTaskControlServer{
		inner:    tc,
		verifier: verifier,
	}
}

type verifiedTaskControlServer struct {
	inner    AuthTaskControlServer
	verifier pb.Verifier
}

func (a *verifiedTaskControlServer) SyncNow(in *SyncNowRequest, stream TaskControl_SyncNowServer) error {
	if ctx, cancel, claims, err := a.verifier.Verify(stream.Context(), pb.Capability_READ); err != nil {
		return err
	} else {
		defer cancel()
		return a.inner.SyncNow(claims, in, verifiedSyncNowStream{ctx, stream})
	}
}

var _ TaskControlServer = &verifiedTaskControlServer{}

type verifiedSyncNowStream struct {
	ctx context.Context
	TaskControl_SyncNowServer
}

func (s verifiedSyncNowStream) Context() context.Context { return s.ctx }
