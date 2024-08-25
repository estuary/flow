package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/golang-jwt/jwt/v5"
	"go.gazette.dev/core/auth"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/metadata"
)

// ControlPlaneAuthorizer is a pb.Authorizer which obtains tokens and
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
type ControlPlaneAuthorizer struct {
	controlAPI    pb.Endpoint
	dataplaneFQDN string
	delegate      *auth.KeyedAuth

	cache struct {
		m  map[authCacheKey]authCacheValue
		mu sync.Mutex
	}
}

// NewControlPlaneAuthorizer returns a ControlPlaneAuthorizer which uses the
// given `controlAPI` endpoint to obtain authorizations, in the context of
// this `dataplaneFQDN` and the `delegate` KeyedAuth which is capable of
// signing tokens for `dataplaneFQDN`.
func NewControlPlaneAuthorizer(delegate *auth.KeyedAuth, dataplaneFQDN string, controlAPI pb.Endpoint) *ControlPlaneAuthorizer {
	var a = &ControlPlaneAuthorizer{
		controlAPI:    controlAPI,
		dataplaneFQDN: dataplaneFQDN,
		delegate:      delegate,
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

func (v *authCacheValue) apply(ctx context.Context) (context.Context, error) {
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

func (a *ControlPlaneAuthorizer) Authorize(ctx context.Context, claims pb.Claims, exp time.Duration) (context.Context, error) {
	var name = claims.Selector.Include.ValueOf("name")

	// Authorizations to shard recovery logs are self-signed.
	if strings.HasPrefix(name, "recovery/") {
		return a.delegate.Authorize(ctx, claims, exp)
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
		return value.apply(ctx)
	}

	// We must issue a new request to the authorization server.
	// Begin by self-signing our request as a JWT.

	claims.Issuer = a.dataplaneFQDN
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
	if token, address, expiresAt, err := doAuthFetch(a.controlAPI, claims, a.delegate.Keys[0]); err != nil {
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

	return value.apply(ctx)
}

func doAuthFetch(controlAPI pb.Endpoint, claims pb.Claims, key jwt.VerificationKey) (string, pb.Endpoint, time.Time, error) {
	var token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to self-sign authorization request: %w", err)
	}
	token = `{"token":"` + token + `"}`

	// Invoke the authorization API.
	var url = controlAPI.URL()
	url.Path = path.Join(url.Path, "/authorize/task")

	httpResp, err := http.Post(url.String(), "application/json", strings.NewReader(token))
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to POST to authorization API: %w", err)
	}
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to read authorization API response: %w", err)
	}
	if httpResp.StatusCode != 200 {
		return "", "", time.Time{}, fmt.Errorf("authorization failed (%s): %s %s", httpResp.Status, string(respBody), token)
	}

	var response struct {
		Token         string
		BrokerAddress pb.Endpoint
	}
	if err = json.Unmarshal(respBody, &response); err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to decode authorization response: %w", err)
	}
	token = response.Token

	claims = pb.Claims{}
	if _, _, err = jwt.NewParser().ParseUnverified(token, &claims); err != nil {
		return "", "", time.Time{}, fmt.Errorf("authorization server returned invalid token: %w", err)
	}

	if claims.Issuer == "" {
		return "", "", time.Time{}, fmt.Errorf("authorization server did not include an issuer claim")
	} else if claims.ExpiresAt == nil {
		return "", "", time.Time{}, fmt.Errorf("authorization server did not include an expires-at claim")
	}

	return token, response.BrokerAddress, claims.ExpiresAt.Time, nil
}

var _ pb.Authorizer = &ControlPlaneAuthorizer{}
