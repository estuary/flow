package runtime

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/golang-jwt/jwt/v5"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/metadata"
)

// controlPlaneAuthorizer is a pb.Authorizer which obtains tokens and
// data-plane endpoints through the Estuary Authorization API.
//
// Specifically it:
//  1. Extends the requested claims with an additional AUTHORIZE capability,
//     the task shard as the Subject, and this data-plane as the Issuer.
//  2. Signs the using this data-plane's key
//  3. Submits this token to the Authorization API for validation and
//     and evaluation of authorization rules.
//  4. Caches and re-uses the authorization result (success or failure)
//     until its expiration.
type controlPlaneAuthorizer struct {
	controlPlane *controlPlane

	cache struct {
		m  map[authCacheKey]authCacheValue
		mu sync.Mutex
	}
}

// newControlPlaneAuthorizer returns a controlPlaneAuthorizer which uses the
// given `controlAPI` endpoint to obtain authorizations.
func newControlPlaneAuthorizer(cp *controlPlane) *controlPlaneAuthorizer {
	var a = &controlPlaneAuthorizer{
		controlPlane: cp,
	}
	a.cache.m = make(map[authCacheKey]authCacheValue)

	return a
}

type authCacheKey struct {
	shardID    string
	name       string
	capability pb.Capability
}

type authCacheValue struct {
	address pb.Endpoint
	err     error
	expires time.Time
	token   string
}

func (v *authCacheValue) apply(ctx context.Context, key authCacheKey) (context.Context, error) {
	if v.err != nil {

		// Special case (hack) for task migration:
		// A newly-migrated task will recover an ACK intent which commits its last
		// stats document to its stats partition. However, as it's migrated to a
		// different data-plane, it's no longer authorized to its old partition.
		// Handle this by returning JOURNAL_NOT_FOUND, which trips logic in the
		// Gazette `consumer` module that discards the ACK intent and continues.
		if key.capability == pb.Capability_APPEND &&
			(strings.HasPrefix(key.name, "ops/tasks/") || strings.HasPrefix(key.name, "ops.us-central1.v1/stats")) &&
			strings.Contains(v.err.Error(), "403 Forbidden") {
			return nil, client.ErrJournalNotFound
		}

		return nil, v.err
	}

	// Often the request will already have an opinion on routing due to dynamic
	// route discovery. If it doesn't, then inject the advertised default service
	// address of the resolved data-plane.
	if route, _, ok := pb.GetDispatchRoute(ctx); !ok || len(route.Members) == 0 {
		ctx = pb.WithDispatchRoute(ctx, pb.Route{
			Members:   []pb.ProcessSpec_ID{{Zone: "", Suffix: string(v.address)}},
			Primary:   0,
			Endpoints: []pb.Endpoint{pb.Endpoint(v.address)},
		}, pb.ProcessSpec_ID{})
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", v.token), nil
}

func (a *controlPlaneAuthorizer) Authorize(ctx context.Context, claims pb.Claims, exp time.Duration) (context.Context, error) {
	var name = claims.Selector.Include.ValueOf("name")

	// Authorizations to shard recovery logs are self-signed.
	if strings.HasPrefix(name, "recovery/") {
		return a.controlPlane.keyedAuth.Authorize(ctx, claims, exp)
	}

	var shardID, ok = pprof.Label(ctx, "shard")
	if !ok {
		panic("missing shard pprof label")
	}

	var key = authCacheKey{
		shardID:    shardID,
		name:       name,
		capability: claims.Capability,
	}

	a.cache.mu.Lock()
	value, ok := a.cache.m[key]
	a.cache.mu.Unlock()

	var now = time.Now()

	// Respond with a cached result, if available.
	// Note that we cache and return errors for a period of time, to avoid any potential
	// accidental DoS of the authorization server due to a thundering herd.
	if ok && !value.expires.Before(now) {
		return value.apply(ctx, key)
	}

	// Fail-fast if the context is already done.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// We must issue a new request to the authorization server.
	// Begin by self-signing our request as a JWT.

	claims.Subject = shardID
	claims.Capability |= pf.Capability_AUTHORIZE // Required for delegated authorization.
	claims.IssuedAt = &jwt.NumericDate{Time: now}
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(exp))

	// Go's `json` encoding is incorrect with respect to canonical
	// protobuf JSON encoding. This patches the encoding so it's conformant
	// (explicit `null` is not allowed).
	if claims.Selector.Include.Labels == nil {
		claims.Selector.Include.Labels = []pb.Label{}
	}
	if claims.Selector.Exclude.Labels == nil {
		claims.Selector.Exclude.Labels = []pb.Label{}
	}

	// Attempt to fetch an authorization token from the control plane.
	// Cache errors for a period of time to prevent thundering herds on errors.
	if token, address, expiresAt, err := doAuthFetch(a.controlPlane, claims); err != nil {
		value = authCacheValue{
			address: "",
			err:     err,
			expires: now.Add(time.Minute),
			token:   "",
		}
	} else {
		value = authCacheValue{
			address: address,
			err:     nil,
			expires: expiresAt,
			token:   fmt.Sprintf("Bearer %s", token),
		}
	}

	a.cache.mu.Lock()
	a.cache.m[key] = value
	a.cache.mu.Unlock()

	return value.apply(ctx, key)
}

func doAuthFetch(cp *controlPlane, claims pb.Claims) (string, pb.Endpoint, time.Time, error) {
	reqToken, err := cp.signClaims(claims)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to self-sign authorization request: %w", err)
	}

	var request = struct {
		Token string `json:"token"`
	}{reqToken}

	var response struct {
		Token         string
		BrokerAddress pb.Endpoint
		RetryMillis   uint64
	}

	// We intentionally use context.Background and not the request context
	// because we cache authorizations.
	if err = callControlAPI(context.Background(), cp, "/authorize/task", &request, &response); err != nil {
		return "", "", time.Time{}, err
	}

	claims = pb.Claims{}
	if _, _, err = jwt.NewParser().ParseUnverified(response.Token, &claims); err != nil {
		return "", "", time.Time{}, fmt.Errorf("authorization server returned invalid token: %w", err)
	}

	if claims.Issuer == "" {
		return "", "", time.Time{}, fmt.Errorf("authorization server did not include an issuer claim")
	} else if claims.ExpiresAt == nil {
		return "", "", time.Time{}, fmt.Errorf("authorization server did not include an expires-at claim")
	}

	return response.Token, response.BrokerAddress, claims.ExpiresAt.Time, nil
}

var _ pb.Authorizer = &controlPlaneAuthorizer{}
