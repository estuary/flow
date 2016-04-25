package gazette

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"

	gc "github.com/go-check/check"

	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/journal"
)

type ReplicateClientSuite struct {
	server *httptest.Server
	client ReplicateClient
}

func (s *ReplicateClientSuite) SetUpSuite(c *gc.C) {
	s.server = httptest.NewServer(s)
	s.client = NewReplicateClient(&discovery.Endpoint{BaseURL: s.server.URL})
}

func (s *ReplicateClientSuite) TearDownSuite(c *gc.C) {
	s.server.Close()
}

func (s *ReplicateClientSuite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/a/journal" {
		http.Error(w, "wrong journal", http.StatusBadRequest)
		return
	}
	query := r.URL.Query()
	if query.Get("newSpool") != "true" {
		http.Error(w, "expected new spool", http.StatusBadRequest)
		return
	}
	if query.Get("writeHead") != "123456" { // Base 10.
		w.Header().Add(WriteHeadHeader, "1e240") // Base 16.
		http.Error(w, "wrong write head", http.StatusBadRequest)
		return
	}
	if query.Get("routeToken") == "network error" {
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
		return
	}
	if query.Get("routeToken") != "a-route-token" {
		http.Error(w, "wrong route token", http.StatusBadRequest)
		return
	}
	// Begin reading the body. This triggers a 100-continue response.
	var body bytes.Buffer
	if _, err := io.Copy(&body, r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if body.String() == "network error" {
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
		return
	}
	if body.String() != "expected write body" {
		http.Error(w, "unexpected body", http.StatusInternalServerError)
		return
	}
	if r.Trailer.Get(CommitDeltaHeader) != "929" { // Base 16.
		http.Error(w, "unexpected commit delta", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *ReplicateClientSuite) opFixture() journal.ReplicateOp {
	return journal.ReplicateOp{
		ReplicateArgs: journal.ReplicateArgs{
			Journal:    "a/journal",
			RouteToken: "a-route-token",
			WriteHead:  123456,
			NewSpool:   true,
		},
		Result: make(chan journal.ReplicateResult),
	}
}

func (s *ReplicateClientSuite) TestSuccess(c *gc.C) {
	var op = s.opFixture()
	s.client.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.IsNil)
	c.Assert(result.Writer, gc.Not(gc.IsNil))

	result.Writer.Write([]byte("expected write body"))
	c.Check(result.Writer.Commit(2345), gc.IsNil)
}

func (s *ReplicateClientSuite) TestIncorrectWriteOffsetHandling(c *gc.C) {
	var op = s.opFixture()
	op.WriteHead = 123
	s.client.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.ErrorMatches, "wrong write head\n")
	c.Check(result.ErrorWriteHead, gc.Equals, int64(123456))
	c.Check(result.Writer, gc.IsNil)
}

func (s *ReplicateClientSuite) TestRequestNetworkErrorHandling(c *gc.C) {
	var op = s.opFixture()
	op.RouteToken = "network error"
	s.client.Replicate(op)

	result := <-op.Result
	c.Check(result.Error, gc.ErrorMatches, "unexpected EOF")
	c.Check(result.Writer, gc.IsNil)
}

func (s *ReplicateClientSuite) TestResponseNetworkErrorHandling(c *gc.C) {
	var op = s.opFixture()
	s.client.Replicate(op)

	result := <-op.Result

	result.Writer.Write([]byte("network error"))
	err := result.Writer.Commit(2345)
	c.Check(err, gc.ErrorMatches, "unexpected EOF")
}

func (s *ReplicateClientSuite) TestErrorResponseHandling(c *gc.C) {
	var op = s.opFixture()
	s.client.Replicate(op)

	result := <-op.Result

	result.Writer.Write([]byte("unexpected write body"))
	err := result.Writer.Commit(2345)
	c.Check(err, gc.ErrorMatches, "unexpected body\n")
}

func (s *ReplicateClientSuite) TestOpParallelism(c *gc.C) {
	var op1, op2 = s.opFixture(), s.opFixture()

	s.client.Replicate(op1)
	result1 := <-op1.Result

	s.client.Replicate(op2)
	result2 := <-op2.Result

	result2.Writer.Write([]byte("expected write body"))
	c.Check(result2.Writer.Commit(2345), gc.IsNil)

	result1.Writer.Write([]byte("expected write body"))
	c.Check(result1.Writer.Commit(2345), gc.IsNil)
}

var _ = gc.Suite(&ReplicateClientSuite{})
