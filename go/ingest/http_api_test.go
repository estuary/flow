package ingest

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
)

func testHTTPSimple(t *testing.T, addr string) {
	var valid = `
	{
		"testing/int-string": [
			{"i": 32, "s": "hello"},
			{"i": 42, "s": "world"}
		]
	}
	`

	var resp, err = http.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(valid))
	require.NoError(t, err)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))

	var out struct {
		Offsets pb.Offsets
		Etcd    pb.Header_Etcd
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
}

func testHTTPNotFound(t *testing.T, addr string) {
	var missing = `
	{"not/found": [{"i": 32, "s": "hello"}]}
	`

	var resp, err = http.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(missing))
	require.NoError(t, err)

	require.Equal(t, 400, resp.StatusCode)
	var body, _ = ioutil.ReadAll(resp.Body)
	require.Equal(t, "\"not/found\" is not an ingestable collection\n", string(body))
}

func testHTTPMalformed(t *testing.T, addr string) {
	var malformed = `
	{"bad": [,{"i": 32,
	`

	var resp, err = http.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(malformed))
	require.NoError(t, err)

	require.Equal(t, 400, resp.StatusCode)
	var body, _ = ioutil.ReadAll(resp.Body)
	require.Equal(t, "invalid character ',' looking for beginning of value\n", string(body))
}
