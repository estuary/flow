package network

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/metadata"
)

// verifyAuthorization ensures the request has an authorization which
// is valid for capability NETWORK_PROXY to `taskName`.
func verifyAuthorization(req *http.Request, verifier pb.Verifier, taskName string) error {
	var bearer = req.Header.Get("authorization")
	if bearer != "" {
		// Pass.
	} else if cookie, err := req.Cookie(AuthCookieName); err == nil {
		bearer = fmt.Sprintf("Bearer %s", cookie.Value)
	} else {
		return errors.New("missing authorization")
	}

	var _, cancel, claims, err = verifier.Verify(
		metadata.NewIncomingContext(
			req.Context(),
			metadata.Pairs("authorization", bearer),
		),
		pf.Capability_NETWORK_PROXY,
	)
	if err != nil {
		return err
	}
	cancel() // We don't use the returned context.

	if !claims.Selector.Matches(pb.MustLabelSet(
		labels.TaskName, taskName,
	)) {
		return fmt.Errorf("invalid authorization for task %s (%s)", taskName, bearer)
	}

	return nil
}

// startAuthRedirect redirect an interactive user to the dashboard, which will
// obtain a user task authorization and redirect back to us with it.
func startAuthRedirect(w http.ResponseWriter, req *http.Request, err error, dashboard *url.URL, taskName string) {
	var query = make(url.Values)
	query.Add("orig_url", "https://"+req.Host+req.URL.Path)
	query.Add("task", taskName)
	query.Add("prefix", taskName)
	query.Add("err", err.Error()) // Informational.

	var target = dashboard.JoinPath("/data-plane-auth-req")
	target.RawQuery = query.Encode()

	http.Redirect(w, req, target.String(), http.StatusTemporaryRedirect)
}

// completeAuthRedirect handles path "/auth-redirect" as part of a redirect chain
// back from the dashboard. It expects a token parameter, which is set as a cookie,
// and an original URL which it in-turn redirects to.
func completeAuthRedirect(w http.ResponseWriter, req *http.Request) {
	var params = req.URL.Query()

	var token = params.Get("token")
	if token == "" {
		http.Error(w, "URL is missing required `token` parameter", http.StatusBadRequest)
		return
	}
	var origUrl = params.Get("orig_url")
	if origUrl == "" {
		http.Error(w, "URL is missing required `orig_url` parameter", http.StatusBadRequest)
		return
	}

	var cookie = &http.Cookie{
		Name:     AuthCookieName,
		Value:    token,
		Secure:   true,
		HttpOnly: true,
		Path:     "/",
	}
	http.SetCookie(w, cookie)

	http.Redirect(w, req, origUrl, http.StatusTemporaryRedirect)
}

func scrubProxyRequest(req *http.Request, public bool) {
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", "") // Omit auto-added User-Agent.
	}

	if public {
		return // All done.
	}

	// Scrub authentication token(s) from the request.
	req.Header.Del("Authorization")

	// There's no `DeleteCookie` function, so we parse them, delete them all, and
	// add them back in while filtering out the flow_auth cookie.
	var cookies = req.Cookies()
	req.Header.Del("Cookie")

	for _, cookie := range cookies {
		if cookie.Name != AuthCookieName {
			req.AddCookie(cookie)
		}
	}
}

// AuthCookieName is the name of the cookie that we use for passing the JWT for interactive logins.
// It's name begins with '__Host-' in order to opt in to some additional security restrictions.
// See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Set-Cookie#cookie_prefixes
const AuthCookieName = "__Host-flow_auth"
