package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/golang-jwt/jwt/v5"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/auth"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/metadata"
)

type ControlPlaneAuthorizer struct {
	authAPI   string
	dataplane string
	delegate  *auth.KeyedAuth

	cache struct {
		m  map[authCacheKey]authCacheValue
		mu sync.Mutex
	}
}

func NewControlPlaneAuthorizer(delegate *auth.KeyedAuth, dataplane string, authAPI string) *ControlPlaneAuthorizer {
	var a = &ControlPlaneAuthorizer{
		authAPI:   authAPI,
		dataplane: dataplane,
		delegate:  delegate,
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
	token   string
	address string
	err     error
	expires time.Time
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

	if !ok || value.expires.Before(now) {
		// Must refresh.
	} else if value.err != nil {
		// Return a cached error for a period of time, to avoid any potential
		// accidental DoS of the authorization server due to a thundering herd.
		return nil, value.err
	} else {
		return metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", value.token)), nil
	}

	// We must issue a new request to the authorization server.

	// Begin by self-signing our request as a JWT.

	claims.Issuer = a.dataplane
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
	if token, address, expiresAt, err := doAuthFetch(a.authAPI, claims, a.delegate.Keys[0]); err != nil {
		value = authCacheValue{
			token:   "",
			address: "",
			err:     err,
			expires: now.Add(time.Second * 10), // time.Minute),
		}
	} else {
		value = authCacheValue{
			token:   token,
			address: address,
			err:     nil,
			expires: expiresAt,
		}
	}

	a.cache.mu.Lock()
	a.cache.m[key] = value
	a.cache.mu.Unlock()

	if value.err != nil {
		return nil, value.err
	} else {
		return metadata.AppendToOutgoingContext(ctx, "authorization", fmt.Sprintf("Bearer %s", value.token)), nil
	}
}

func doAuthFetch(authAPI string, claims pb.Claims, key jwt.VerificationKey) (string, string, time.Time, error) {
	var token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to self-sign authorization request: %w", err)
	}
	token = `{"token":"` + token + `"}`

	// logrus.WithFields(logrus.Fields{"token": token}).Info("AUTHORIZE REQUEST")

	httpResp, err := http.Post(authAPI, "application/json", strings.NewReader(token))
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to POST to authorization server: %w", err)
	}

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to read authorization server response: %w", err)
	}
	if httpResp.StatusCode != 200 {
		return "", "", time.Time{}, fmt.Errorf("authorization failed (%s): %s %s", httpResp.Status, string(respBody), token)
	}

	var response struct {
		Token         string
		BrokerAddress string
	}
	if err = json.Unmarshal(respBody, &response); err != nil {
		return "", "", time.Time{}, fmt.Errorf("failed to decode response: %w", err)
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

	logrus.WithFields(logrus.Fields{"claims": claims, "token": token, "err": err}).Info("AUTHORIZE RESPONSE")

	return token, response.BrokerAddress, claims.ExpiresAt.Time, nil
}

var _ pb.Authorizer = &ControlPlaneAuthorizer{}
