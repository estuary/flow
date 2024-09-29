package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"path"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.gazette.dev/core/auth"
	pb "go.gazette.dev/core/broker/protocol"
)

type controlPlane struct {
	endpoint      pb.Endpoint
	dataplaneFQDN string
	keyedAuth     *auth.KeyedAuth
}

func newControlPlane(keyedAuth *auth.KeyedAuth, dataplaneFQDN string, controlAPI pb.Endpoint) *controlPlane {
	var cp = &controlPlane{
		endpoint:      controlAPI,
		dataplaneFQDN: dataplaneFQDN,
		keyedAuth:     keyedAuth,
	}
	return cp
}

func (cp *controlPlane) signClaims(claims pb.Claims) (string, error) {
	claims.Issuer = cp.dataplaneFQDN

	// Go's `json` encoding is incorrect with respect to canonical
	// protobuf JSON encoding (explicit `null` is not allowed).
	// This patches the encoding so it's conformant.
	if claims.Selector.Include.Labels == nil {
		claims.Selector.Include.Labels = []pb.Label{}
	}
	if claims.Selector.Exclude.Labels == nil {
		claims.Selector.Exclude.Labels = []pb.Label{}
	}

	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(cp.keyedAuth.Keys[0])
}

func callControlAPI[Request any, Response any](ctx context.Context, cp *controlPlane, resource string, request *Request, response *Response) error {
	var url = cp.endpoint.URL()
	url.Path = path.Join(url.Path, resource)

	var reqBytes, err = json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to encode %s API request body: %w", resource, err)
	}

	for {
		httpReq, err := http.NewRequestWithContext(ctx, "POST", url.String(), bytes.NewReader(reqBytes))
		if err != nil {
			return fmt.Errorf("failed to build POST request to %s: %w", resource, err)
		}
		httpReq.Header.Add("content-type", "application/json")

		httpResp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			return fmt.Errorf("failed to POST to %s API: %w", resource, err)
		}
		respBody, err := io.ReadAll(httpResp.Body)
		if err != nil {
			return fmt.Errorf("failed to read %s API response: %w", resource, err)
		}

		// Parse the response for an indication of whether we should retry.
		var skim struct {
			RetryMillis uint64
		}

		if sc := httpResp.StatusCode; sc >= 500 && sc < 600 {
			skim.RetryMillis = rand.Uint64N(4_750) + 250 // Random backoff in range [0.250s, 5s].
		} else if sc != 200 {
			return fmt.Errorf("%s: %s", httpResp.Status, string(respBody))
		} else if err = json.Unmarshal(respBody, &skim); err != nil {
			return fmt.Errorf("failed to decode %s API response: %w", resource, err)
		} else if skim.RetryMillis != 0 {
			time.Sleep(time.Millisecond * time.Duration(skim.RetryMillis))
		} else if err := json.Unmarshal(respBody, response); err != nil {
			return fmt.Errorf("failed to decode %s API response: %w", resource, err)
		} else {
			return nil
		}
	}
}
