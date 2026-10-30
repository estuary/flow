package protocol

import (
	"context"
	"math"
	"time"

	"github.com/golang-jwt/jwt/v5"
	grpc "google.golang.org/grpc"
)

// Capability is a bit-mask of authorized capabilities to a resource.
type Capability uint32

const (
	Capability_LIST      Capability = 1 << 1
	Capability_APPLY     Capability = 1 << 2
	Capability_READ      Capability = 1 << 3
	Capability_APPEND    Capability = 1 << 4
	Capability_REPLICATE Capability = 1 << 5

	// Applications may use Capability bits starting at 1 << 16.

	Capability_ALL Capability = math.MaxUint32
)

// Claims reflect the scope of an authorization. They grant the client the
// indicated Capability against resources matched by the corresponding
// Selector.
type Claims struct {
	Capability Capability    `json:"cap"`
	Selector   LabelSelector `json:"sel"`
	jwt.RegisteredClaims
}

// WithClaims attaches desired Claims to a Context.
// The attached claims will be used by AuthJournalClient to obtain Authorization tokens.
func WithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsCtxKey{}, claims)
}

// GetClaims retrieves attached Claims from a Context.
func GetClaims(ctx context.Context) (Claims, bool) {
	var claims, ok = ctx.Value(claimsCtxKey{}).(Claims)
	return claims, ok
}

// Authorizer returns a Context which incorporates a gRPC Authorization for the given Claims.
// It could do so by directly signing the claims with a pre-shared key,
// or by requesting a signature from a separate authorization service.
type Authorizer interface {
	Authorize(ctx context.Context, claims Claims, exp time.Duration) (context.Context, error)
}

// Verifier verifies an Authorization token, returning its validated Claims.
type Verifier interface {
	Verify(ctx context.Context, require Capability) (context.Context, context.CancelFunc, Claims, error)
}

// NewAuthJournalClient returns an *AuthJournalClient which uses the Authorizer
// to obtain and attach an Authorization bearer token to every issued request.
func NewAuthJournalClient(jc JournalClient, auth Authorizer) *AuthJournalClient {
	return &AuthJournalClient{Authorizer: auth, Inner: jc}
}

type AuthJournalClient struct {
	Authorizer Authorizer
	Inner      JournalClient
}

func (a *AuthJournalClient) List(ctx context.Context, in *ListRequest, opts ...grpc.CallOption) (Journal_ListClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_LIST,
			Selector:   in.Selector,
		}
	}
	// List blocks if `in.Watch` is set.
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.Inner.List(ctx, in, opts...)
	}
}

func (a *AuthJournalClient) Apply(ctx context.Context, in *ApplyRequest, opts ...grpc.CallOption) (*ApplyResponse, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{Capability: Capability_APPLY}
	}
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Minute); err != nil {
		return nil, err
	} else {
		return a.Inner.Apply(ctx, in, opts...)
	}
}

func (a *AuthJournalClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Journal_ReadClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_READ,
			Selector: LabelSelector{
				Include: MustLabelSet("name", in.Journal.StripMeta().String()),
			},
		}
	}
	// Read blocks if `in.Block` is set.
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.Inner.Read(ctx, in, opts...)
	}
}

func (a *AuthJournalClient) Append(ctx context.Context, opts ...grpc.CallOption) (Journal_AppendClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		panic("Append requires a context having WithClaims")
	}
	// Append may block awaiting fragment store listing and health retries.
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.Inner.Append(ctx, opts...)
	}
}

func (a *AuthJournalClient) Replicate(ctx context.Context, opts ...grpc.CallOption) (Journal_ReplicateClient, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		panic("Replicate requires a context having WithClaims")
	}
	// Replicate is a long-lived bidi stream.
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.Inner.Replicate(ctx, opts...)
	}
}

func (a *AuthJournalClient) ListFragments(ctx context.Context, in *FragmentsRequest, opts ...grpc.CallOption) (*FragmentsResponse, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_READ, // Allows direct fragment access.
			Selector: LabelSelector{
				Include: MustLabelSet("name", in.Journal.StripMeta().String()),
			},
		}
	}
	// ListFragments may block awaiting fragment store listing and health retries.
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.Inner.ListFragments(ctx, in, opts...)
	}
}

func (a *AuthJournalClient) FragmentStoreHealth(ctx context.Context, in *FragmentStoreHealthRequest, opts ...grpc.CallOption) (*FragmentStoreHealthResponse, error) {
	var claims, ok = GetClaims(ctx)
	if !ok {
		claims = Claims{
			Capability: Capability_READ, // Store health requires read access
		}
	}
	// FragmentStoreHealth may block awaiting fragment store listing.
	if ctx, err := a.Authorizer.Authorize(ctx, claims, time.Hour); err != nil {
		return nil, err
	} else {
		return a.Inner.FragmentStoreHealth(ctx, in, opts...)
	}
}

// AuthJournalServer is similar to JournalServer except:
//   - Requests have already been verified with accompanying Claims.
//   - The Context or Stream.Context() argument may be subject to a deadline
//     bound to the expiration of the user's Claims.
type AuthJournalServer interface {
	List(Claims, *ListRequest, Journal_ListServer) error
	Apply(context.Context, Claims, *ApplyRequest) (*ApplyResponse, error)
	Read(Claims, *ReadRequest, Journal_ReadServer) error
	Append(Claims, Journal_AppendServer) error
	Replicate(Claims, Journal_ReplicateServer) error
	ListFragments(context.Context, Claims, *FragmentsRequest) (*FragmentsResponse, error)
	FragmentStoreHealth(context.Context, Claims, *FragmentStoreHealthRequest) (*FragmentStoreHealthResponse, error)
}

// NewVerifiedJournalServer adapts an AuthJournalServer into a JournalServer by
// using the provided Verifier to verify incoming request Authorizations.
func NewVerifiedJournalServer(ajs AuthJournalServer, verifier Verifier) *VerifiedJournalServer {
	return &VerifiedJournalServer{
		Verifier: verifier,
		Inner:    ajs,
	}
}

type VerifiedJournalServer struct {
	Verifier Verifier
	Inner    AuthJournalServer
}

func (s *VerifiedJournalServer) List(req *ListRequest, stream Journal_ListServer) error {
	if ctx, cancel, claims, err := s.Verifier.Verify(stream.Context(), Capability_LIST); err != nil {
		return err
	} else {
		defer cancel()
		return s.Inner.List(claims, req, verifiedListServer{ctx, stream})
	}
}

func (s *VerifiedJournalServer) Apply(ctx context.Context, req *ApplyRequest) (*ApplyResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, Capability_APPLY); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.Apply(ctx, claims, req)
	}
}

func (s *VerifiedJournalServer) Read(req *ReadRequest, stream Journal_ReadServer) error {
	if ctx, cancel, claims, err := s.Verifier.Verify(stream.Context(), Capability_READ); err != nil {
		return err
	} else {
		defer cancel()
		return s.Inner.Read(claims, req, verifiedReadServer{ctx, stream})
	}
}

func (s *VerifiedJournalServer) Append(stream Journal_AppendServer) error {
	if ctx, cancel, claims, err := s.Verifier.Verify(stream.Context(), Capability_APPEND); err != nil {
		return err
	} else {
		defer cancel()
		return s.Inner.Append(claims, verifiedAppendServer{ctx, stream})
	}
}

func (s *VerifiedJournalServer) Replicate(stream Journal_ReplicateServer) error {
	if ctx, cancel, claims, err := s.Verifier.Verify(stream.Context(), Capability_REPLICATE); err != nil {
		return err
	} else {
		defer cancel()
		return s.Inner.Replicate(claims, verifiedReplicateServer{ctx, stream})
	}
}

func (s *VerifiedJournalServer) ListFragments(ctx context.Context, req *FragmentsRequest) (*FragmentsResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.ListFragments(ctx, claims, req)
	}
}

func (s *VerifiedJournalServer) FragmentStoreHealth(ctx context.Context, req *FragmentStoreHealthRequest) (*FragmentStoreHealthResponse, error) {
	if ctx, cancel, claims, err := s.Verifier.Verify(ctx, Capability_READ); err != nil {
		return nil, err
	} else {
		defer cancel()
		return s.Inner.FragmentStoreHealth(ctx, claims, req)
	}
}

var _ JournalServer = &VerifiedJournalServer{}
var _ JournalClient = &AuthJournalClient{}

type claimsCtxKey struct{}

type verifiedListServer struct {
	ctx context.Context
	Journal_ListServer
}
type verifiedReadServer struct {
	ctx context.Context
	Journal_ReadServer
}
type verifiedAppendServer struct {
	ctx context.Context
	Journal_AppendServer
}
type verifiedReplicateServer struct {
	ctx context.Context
	Journal_ReplicateServer
}

func (s verifiedListServer) Context() context.Context      { return s.ctx }
func (s verifiedReadServer) Context() context.Context      { return s.ctx }
func (s verifiedAppendServer) Context() context.Context    { return s.ctx }
func (s verifiedReplicateServer) Context() context.Context { return s.ctx }
