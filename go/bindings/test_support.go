package bindings

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

// TestLambdaHandler is the signature of a test handler that receives lambda invocations.
// It returns an array of arbitrary values which are incorporated into the JSON-encoded
// lambda response body.
type TestLambdaHandler = func(source, previous, register json.RawMessage) ([]interface{}, error)

// NewTestLambdaServer builds and returns an HTTP/2 cleartext ("h2c") server of the provided
// |routes| handlers, listening on the returned unix socket path. It emulates the behavior of
// Flow's TypeScript runtime server, and is a stand-in within Go tests.
func NewTestLambdaServer(t *testing.T, routes map[string]TestLambdaHandler) (_ *http.Client, stop func()) {
	var handler = func(w http.ResponseWriter, r *http.Request) (out []byte, err error) {
		defer func() {
			if err != nil {
				w.WriteHeader(500)
				out = []byte(err.Error())
			} else {
				w.WriteHeader(200)
			}
			w.Write(out)
		}()

		var sources []json.RawMessage
		var registers []invokeRegister

		// Parse tuple of [sources, registers], where registers may be missing.
		err = json.NewDecoder(r.Body).Decode(&[]interface{}{&sources, &registers})
		if err != nil {
			return nil, fmt.Errorf("reading request %#v: %w", r, err)
		}

		var handler, handlerOK = routes[r.URL.Path]
		if !handlerOK {
			return nil, fmt.Errorf("handler %q not found", r.URL.Path)
		}

		var bodyRows []interface{}
		for row := range sources {
			var body interface{}
			var err error

			if len(registers) == 0 {
				body, err = handler(sources[row], nil, nil)
			} else {
				body, err = handler(sources[row], registers[row].Previous, registers[row].Register)
			}

			if err != nil {
				return nil, err
			}
			bodyRows = append(bodyRows, body)
		}

		if out, err = json.Marshal(bodyRows); err != nil {
			return nil, fmt.Errorf("marshaling response: %w", err)
		}
		return
	}

	var path = filepath.Join(t.TempDir(), "socket")
	listener, err := net.Listen("unix", path)
	require.NoError(t, err)

	go func() {
		for {
			var conn, err = listener.Accept()
			if err != nil {
				t.Logf("test lambda server exiting with Accept error: %s", err)
				return
			}

			(&http2.Server{}).ServeConn(conn, &http2.ServeConnOpts{
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler(w, r) }),
			})
		}
	}()

	var client = &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(_, _ string, _ *tls.Config) (net.Conn, error) {
				return net.Dial("unix", path)
			},
		},
	}

	return client, func() {
		require.NoError(t, listener.Close())
	}
}

// invokeRegister facilitate parsing previous & current register values from invocation
// bodies. It's UnmarshalJSON works around (lack of) Go support for parsing tuples.
type invokeRegister struct {
	Previous, Register json.RawMessage
}

func (r *invokeRegister) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, &[]interface{}{&r.Previous, &r.Register})
}
