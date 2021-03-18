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

func testHTTPMultiSimple(t *testing.T, addr string) {
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

func testHTTPMultiNotFound(t *testing.T, addr string) {
	var missing = `
	{"not/found": [{"i": 32, "s": "hello"}]}
	`

	var resp, err = http.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(missing))
	require.NoError(t, err)

	require.Equal(t, 400, resp.StatusCode)
	var body, _ = ioutil.ReadAll(resp.Body)
	require.Equal(t, "fetching specification for \"not/found\": not found\n", string(body))
}

func testHTTPMultiMalformed(t *testing.T, addr string) {
	var malformed = `
	{"bad": [,{"i": 32,
	`

	var resp, err = http.Post("http://"+addr+"/ingest", "application/json", strings.NewReader(malformed))
	require.NoError(t, err)

	require.Equal(t, 400, resp.StatusCode)
	var body, _ = ioutil.ReadAll(resp.Body)
	require.Equal(t, "invalid character ',' looking for beginning of value\n", string(body))
}

func testHTTPSingleSimple(t *testing.T, addr string) {
	var valid = `{"i": 42, "s": "world"}`

	var resp, err = http.Post("http://"+addr+"/ingest/testing/int-string", "application/json", strings.NewReader(valid))
	require.NoError(t, err)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("content-type"))

	var out struct {
		Offsets pb.Offsets
		Etcd    pb.Header_Etcd
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
}

func testHTTPSingleNotFound(t *testing.T, addr string) {
	var theBodySaysYes = `{"i": 42, "s": "world"}`

	var resp, err = http.Post("http://"+addr+"/ingest/the/mind/says/no", "application/json", strings.NewReader(theBodySaysYes))
	require.NoError(t, err)

	require.Equal(t, 400, resp.StatusCode)
	var body, _ = ioutil.ReadAll(resp.Body)
	require.Equal(t, "fetching specification for \"the/mind/says/no\": not found\n", string(body))
}

func testHTTPSingleMalformed(t *testing.T, addr string) {
	var malformed = `
	{"bad": [,{"i": 32,
	`

	var resp, err = http.Post("http://"+addr+"/ingest/testing/int-string", "application/json", strings.NewReader(malformed))
	require.NoError(t, err)

	require.Equal(t, 400, resp.StatusCode)
	var body, _ = ioutil.ReadAll(resp.Body)
	require.Equal(t, "ingestion of collection \"testing/int-string\": JSON error: expected value at line 2 column 11\n\nCaused by:\n    expected value at line 2 column 11\n", string(body))
}
