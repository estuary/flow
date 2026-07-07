package flow

import (
	context "context"
	"time"

	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

const (
	// AUTHORIZE gives the bearer a capability to request an authorization
	// for the given claim, which may then be signed using a different key
	// and returned without the AUTHORIZE capability (which prevents the
	// recipient from using the token to obtain further Authorizations).
	Capability_AUTHORIZE pb.Capability = 1 << 16
	// SHUFFLE gives the bearer a capability to use the runtime's Shuffle API.
	Capability_SHUFFLE pb.Capability = 1 << 17
	// NETWORK_PROXY gives the bearer a capability to use the runtime's Network Proxy API.
	Capability_NETWORK_PROXY pb.Capability = 1 << 18
	// CONNECTOR gives the bearer a capability to use the runtime's connector APIs.
	Capability_PROXY_CONNECTOR pb.Capability = 1 << 19
	// LEAD gives the bearer a capability to use the runtime's Leader API,
	// driving a derivation or materialization task's transactions.
	Capability_LEAD pb.Capability = 1 << 20
)

// NewAuthNetworkProxyClient returns a NetworkProxyClient which uses the Authorizer
// to obtain and attach an Authorization bearer token to every issued request.
func NewAuthNetworkProxyClient(npc NetworkProxyClient, auth pb.Authorizer) NetworkProxyClient {
	return &authNetworkProxyClient{auth: auth, npc: npc}
}

type authNetworkProxyClient struct {
	auth pb.Authorizer
	npc  NetworkProxyClient
}

func (a *authNetworkProxyClient) Proxy(ctx context.Context, opts ...grpc.CallOption) (NetworkProxy_ProxyClient, error) {
	var claims, ok = pb.GetClaims(ctx)
	if !ok {
		panic("Proxy requires a context having WithClaims")
	}
	if ctx, err := a.auth.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.npc.Proxy(ctx, opts...)
	}
}

// AuthNetworkProxyServer is similar to NetworkProxyServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context or Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthNetworkProxyServer interface {
	Proxy(claims pb.Claims, stream NetworkProxy_ProxyServer) error
}

// NewVerifiedNetworkProxyServer adapts an AuthNetworkProxyServer into a NetworkProxyServer by
// using the provided Verifier to verify incoming request Authorizations.
func NewVerifiedNetworkProxyServer(npc AuthNetworkProxyServer, verifier pb.Verifier) NetworkProxyServer {
	return &verifiedNetworkProxyServer{
		inner:    npc,
		verifier: verifier,
	}
}

type verifiedNetworkProxyServer struct {
	inner    AuthNetworkProxyServer
	verifier pb.Verifier
}

func (a *verifiedNetworkProxyServer) Proxy(stream NetworkProxy_ProxyServer) error {
	if ctx, cancel, claims, err := a.verifier.Verify(stream.Context(), Capability_NETWORK_PROXY); err != nil {
		return err
	} else {
		defer cancel()
		return a.inner.Proxy(claims, verifiedProxyStream{ctx, stream})
	}
}

var _ NetworkProxyServer = &verifiedNetworkProxyServer{}
var _ NetworkProxyClient = &authNetworkProxyClient{}

type verifiedProxyStream struct {
	ctx context.Context
	NetworkProxy_ProxyServer
}

func (s verifiedProxyStream) Context() context.Context { return s.ctx }

// AuthSyncNowServer is similar to SyncNowServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context argument may be subject to a deadline bound to the
//     expiration of the user's Claims.
type AuthSyncNowServer interface {
	SyncNow(claims pb.Claims, ctx context.Context, req *SyncNowRequest) (*SyncNowResponse, error)
}

// NewVerifiedSyncNowServer adapts an AuthSyncNowServer into a SyncNowServer by
// using the provided Verifier to verify incoming request Authorizations.
//
// Sync-now forces a task to commit its pending writes, so it requires the
// gazette APPEND capability — the capability implied by a control-plane Write
// grant (see control-plane-api `map_capability_to_gazette`). Per-shard scoping
// is then enforced by the handler's Resolve, which checks the Claims selector
// against the resolved shard.
func NewVerifiedSyncNowServer(inner AuthSyncNowServer, verifier pb.Verifier) SyncNowServer {
	return &verifiedSyncNowServer{
		inner:    inner,
		verifier: verifier,
	}
}

type verifiedSyncNowServer struct {
	inner    AuthSyncNowServer
	verifier pb.Verifier
}

func (a *verifiedSyncNowServer) SyncNow(ctx context.Context, req *SyncNowRequest) (*SyncNowResponse, error) {
	if ctx, cancel, claims, err := a.verifier.Verify(ctx, pb.Capability_APPEND); err != nil {
		return nil, err
	} else {
		defer cancel()
		return a.inner.SyncNow(claims, ctx, req)
	}
}

var _ SyncNowServer = &verifiedSyncNowServer{}
