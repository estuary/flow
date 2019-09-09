package http_gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/broker/teststub"
)

type HTTPSuite struct{}

func (s *HTTPSuite) TestReadRequestParsing(c *gc.C) {
	var g = NewGateway(nil)

	var cases = []struct {
		method, url string
		err         string
		rr          pb.ReadRequest
	}{
		{method: "GET", url: "/journal/name", rr: pb.ReadRequest{
			Journal: "journal/name"}},
		{method: "GET", url: "/journal/name?block=true", rr: pb.ReadRequest{
			Journal: "journal/name", Block: true}},
		{method: "GET", url: "/journal/name?offset=123&block=true", rr: pb.ReadRequest{
			Journal: "journal/name", Offset: 123, Block: true}},
		{method: "HEAD", url: "/journal/name?offset=123&block=true", rr: pb.ReadRequest{
			Journal: "journal/name", Offset: 123, Block: true, MetadataOnly: true}},

		// Validation errors.
		{method: "GET", url: "/journal/name?offset=-2",
			err: `invalid Offset \(-2; .*`},
		{method: "GET", url: "/journal//name",
			err: `Journal: must be a clean path \(journal//name\)`},

		// Schema decoding errors.
		{method: "GET", url: "/journal/name?block=foobar",
			err: `schema: error converting value for "block"`},
		{method: "GET", url: "/journal/name?offset=foobar",
			err: `schema: error converting value for "offset"`},
		{method: "GET", url: "/journal/name?extra=1",
			err: `schema: invalid path "extra"`},
	}

	for _, tc := range cases {
		var req, _ = http.NewRequest(tc.method, tc.url, nil)
		var rr, err = g.parseReadRequest(req)

		if tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		} else {
			c.Check(rr, gc.DeepEquals, tc.rr)
		}
	}
}

func (s *HTTPSuite) TestAppendRequestParsing(c *gc.C) {
	var g = NewGateway(nil)

	var cases = []struct {
		url string
		err string
		ar  pb.AppendRequest
	}{
		{url: "/journal/name", ar: pb.AppendRequest{Journal: "journal/name"}},
		// Validation error.
		{url: "/journal//name", err: `Journal: must be a clean path \(journal//name\)`},
		// Schema decoding error.
		{url: "/journal/name?extra=1", err: `schema: invalid path "extra"`},
	}

	for _, tc := range cases {
		var req, _ = http.NewRequest("PUT", tc.url, nil)
		var ar, err = g.parseAppendRequest(req)

		if tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		} else {
			c.Check(ar, gc.DeepEquals, tc.ar)
		}
	}
}

func (s *HTTPSuite) TestWriteReadResponse(c *gc.C) {
	var req, _ = http.NewRequest("GET", "/a/journal", nil)

	var w = httptest.NewRecorder()
	writeReadResponse(w, req, readResponseFixture)

	c.Check(w.Header(), gc.DeepEquals, http.Header{
		"Content-Range":            []string{"bytes 1024-9223372036854775807/9223372036854775807"},
		"Location":                 []string{"http://broker/path/a/journal"},
		"X-Fragment-Last-Modified": []string{"Sat, 23 May 1970 21:21:18 GMT"},
		"X-Fragment-Location":      []string{"http://host/path/to/fragment"},
		"X-Fragment-Name":          []string{"00000000000003e8-00000000000005dc-0000000000002694000000000000000000000000.sz"},
		"X-Route-Token":            []string{"members:<zone:\"a\" suffix:\"broker\" > endpoints:\"http://broker/path\" "},
		"X-Write-Head":             []string{"2048"},
		"Trailer":                  []string{"X-Close-Error"}, // Declared header to be sent as trailer.
	})
}

func (s *HTTPSuite) TestWriteAppendResponse(c *gc.C) {
	var req, _ = http.NewRequest("PUT", "/a/journal", nil)

	var w = httptest.NewRecorder()
	writeAppendResponse(w, req, appendResponseFixture)

	c.Check(w.Header(), gc.DeepEquals, http.Header{
		"Location":          []string{"http://broker/path/a/journal"},
		"X-Commit-Begin":    []string{"100"},
		"X-Commit-End":      []string{"200"},
		"X-Commit-Sha1-Sum": []string{"0000000000002694000000000000000000000000"},
		"X-Route-Token":     []string{"members:<zone:\"a\" suffix:\"broker\" > endpoints:\"http://broker/path\" "},
		"X-Write-Head":      []string{"200"},
	})
}

func (s *HTTPSuite) TestServingRead(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var g = NewGateway(rjc)

	go func() {
		c.Check(<-broker.ReadReqCh, gc.DeepEquals, pb.ReadRequest{Journal: "a/journal", Offset: 123})

		broker.ReadRespCh <- readResponseFixture
		broker.ReadRespCh <- pb.ReadResponse{Content: []byte("hello, "), Offset: 1024}
		broker.ReadRespCh <- pb.ReadResponse{Content: []byte("world!"), Offset: 1031}
		broker.WriteLoopErrCh <- nil
	}()

	var req, _ = http.NewRequest("GET", "/a/journal?offset=123", nil)
	var w = httptest.NewRecorder()

	g.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
	c.Check(w.Header()["X-Write-Head"], gc.DeepEquals, []string{"2048"})
	c.Check(w.Header()["X-Close-Error"], gc.DeepEquals, []string{"broker terminated RPC"})
	c.Check(w.Body.String(), gc.Equals, "hello, world!")
	c.Check(w.Flushed, gc.Equals, true)
}

func (s *HTTPSuite) TestServingWrite(c *gc.C) {
	var broker = teststub.NewBroker(c)
	defer broker.Cleanup()

	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})
	var g = NewGateway(rjc)

	go func() {
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Journal: "a/journal"})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{Content: []byte("some content")})
		c.Check(<-broker.AppendReqCh, gc.DeepEquals, pb.AppendRequest{})
		c.Check(<-broker.ReadLoopErrCh, gc.Equals, io.EOF)

		broker.AppendRespCh <- appendResponseFixture
	}()

	var req, _ = http.NewRequest("PUT", "/a/journal", strings.NewReader("some content"))
	var w = httptest.NewRecorder()

	g.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusNoContent)
	c.Check(w.Body.String(), gc.Equals, "")
	c.Check(w.Header()["X-Write-Head"], gc.DeepEquals, []string{"200"})
}

var (
	_ = gc.Suite(&HTTPSuite{})

	readResponseFixture = pb.ReadResponse{
		Status: pb.Status_OK,
		Header: &pb.Header{
			ProcessId: pb.ProcessSpec_ID{Zone: "a", Suffix: "broker"},
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
				Primary:   0,
				Endpoints: []pb.Endpoint{"http://broker/path"},
			},
			Etcd: pb.Header_Etcd{ClusterId: 12, MemberId: 34, Revision: 56, RaftTerm: 78},
		},
		Offset:    1024,
		WriteHead: 2048,
		Fragment: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            1000,
			End:              1500,
			Sum:              pb.SHA1Sum{Part1: 9876},
			CompressionCodec: pb.CompressionCodec_SNAPPY,
			ModTime:          time.Unix(12345678, 0).Unix(),
		},
		FragmentUrl: "http://host/path/to/fragment",
	}

	appendResponseFixture = pb.AppendResponse{
		Status: pb.Status_OK,
		Header: pb.Header{
			ProcessId: pb.ProcessSpec_ID{Zone: "a", Suffix: "broker"},
			Route: pb.Route{
				Members:   []pb.ProcessSpec_ID{{Zone: "a", Suffix: "broker"}},
				Primary:   0,
				Endpoints: []pb.Endpoint{"http://broker/path"},
			},
			Etcd: pb.Header_Etcd{ClusterId: 12, MemberId: 34, Revision: 56, RaftTerm: 78},
		},
		Commit: &pb.Fragment{
			Journal:          "a/journal",
			Begin:            100,
			End:              200,
			Sum:              pb.SHA1Sum{Part1: 9876},
			CompressionCodec: pb.CompressionCodec_SNAPPY,
		},
		Registers: new(pb.LabelSet),
	}
)

func Test(t *testing.T) { gc.TestingT(t) }
