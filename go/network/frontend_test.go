package network

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"testing"
)

// serveConnErr is the best-effort error path taken by connections that never
// reach the HTTP handler: a raw-address probe with no matching SNI (exactly
// what a security scanner hits) is answered here, not through serveConnHTTP.
// These responses must still carry the HSTS header, so exercise the real
// method and parse the bytes it writes to the connection.
func TestServeConnErrSetsHSTS(t *testing.T) {
	// The 404 (no matching SNI) and 503 (shard dial failure) paths are served
	// over TLS; 421 is served over the raw connection. All flow through here.
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable, http.StatusMisdirectedRequest} {
		var server, client = net.Pipe()

		go (&Frontend{}).serveConnErr(server, status, "an error occurred\n")

		var resp, err = http.ReadResponse(bufio.NewReader(client), nil)
		if err != nil {
			t.Fatalf("status %d: reading response: %v", status, err)
		}
		// Drain the body so serveConnErr's Write completes and it closes `server`.
		var body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()

		if got := resp.Header.Get("Strict-Transport-Security"); got != hstsHeaderValue {
			t.Errorf("status %d: HSTS header = %q, want %q", status, got, hstsHeaderValue)
		}
		if resp.StatusCode != status {
			t.Errorf("status code = %d, want %d", resp.StatusCode, status)
		}
		if string(body) != "an error occurred\n" {
			t.Errorf("status %d: body = %q, want the error text", status, body)
		}
	}
}
