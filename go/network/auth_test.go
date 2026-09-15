package network

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// completeAuthRedirect is dispatched ahead of verifyAuthorization (see
// serveConnHTTP), so any client which can resolve a task hostname reaches it
// with parameters of its choosing. An unpinned `orig_url` would therefore turn
// every task domain into an open redirect, and would hand the auth cookie this
// handler sets to whatever origin the caller named.
func TestCompleteAuthRedirectPinsOrigURLToHost(t *testing.T) {
	for _, tc := range []struct {
		name     string
		origUrl  string
		wantCode string
	}{
		{"same host is allowed", "https://task.example.com/some/path", "307"},
		{"foreign host is rejected", "https://evil.example.com/", "400"},
		{"scheme-relative foreign host is rejected", "//evil.example.com/", "400"},
		{"host-relative path is rejected", "/some/path", "400"},
		{"unparseable URL is rejected", "https://[::1", "400"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req = httptest.NewRequest("GET", "https://task.example.com/auth-redirect", nil)
			var query = req.URL.Query()
			query.Set("token", "a-task-token")
			query.Set("orig_url", tc.origUrl)
			req.URL.RawQuery = query.Encode()

			var rec = httptest.NewRecorder()
			completeAuthRedirect(rec, req)

			var resp = rec.Result()
			if got := resp.Status[:3]; got != tc.wantCode {
				t.Errorf("status = %s, want %s", got, tc.wantCode)
			}

			// A rejected redirect must not leave the caller's token behind as a
			// cookie, since that is half of the attack this check prevents.
			var cookies = resp.Cookies()
			if tc.wantCode == "400" && len(cookies) != 0 {
				t.Errorf("rejected request set %d cookie(s), want none", len(cookies))
			}
			if tc.wantCode != "307" {
				return
			}
			if got := resp.Header.Get("Location"); got != tc.origUrl {
				t.Errorf("Location = %q, want %q", got, tc.origUrl)
			}
			if len(cookies) != 1 || cookies[0].Name != AuthCookieName || cookies[0].Value != "a-task-token" {
				t.Errorf("cookies = %v, want a single %s carrying the token", cookies, AuthCookieName)
			}
		})
	}
}

// The parameters are required, and their absence is reported before any
// redirect or cookie is written.
func TestCompleteAuthRedirectRequiresParameters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"missing token", "orig_url=https://task.example.com/"},
		{"missing orig_url", "token=a-task-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req = httptest.NewRequest("GET", "https://task.example.com/auth-redirect?"+tc.query, nil)
			var rec = httptest.NewRecorder()
			completeAuthRedirect(rec, req)

			var resp = rec.Result()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			if got := len(resp.Cookies()); got != 0 {
				t.Errorf("set %d cookie(s), want none", got)
			}
		})
	}
}
