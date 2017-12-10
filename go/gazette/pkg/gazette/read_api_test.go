package gazette

import (
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	gc "github.com/go-check/check"
	"github.com/gorilla/mux"

	"github.com/LiveRamp/gazette/pkg/cloudstore"
	"github.com/LiveRamp/gazette/pkg/journal"
)

type ReadAPISuite struct {
	localDir string
	spool    *journal.Spool
	mux      *mux.Router
	cfs      cloudstore.FileSystem

	readCallbacks []func(op journal.ReadOp)
}

func (s *ReadAPISuite) SetUpSuite(c *gc.C) {
	// Create a file-backed fragment fixture to return.
	var err error
	s.localDir, err = ioutil.TempDir("", "read-api-suite")
	c.Assert(err, gc.IsNil)

	s.spool, err = journal.NewSpool(s.localDir, journal.Mark{"journal/name", 12345})
	c.Check(err, gc.IsNil)

	n, err := s.spool.Write([]byte("XXXXXexpected read fixture"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 26)
	c.Check(s.spool.Commit(int64(n)), gc.IsNil)

	s.mux = mux.NewRouter()
	s.cfs = cloudstore.NewTmpFileSystem()
	NewReadAPI(s, s.cfs).Register(s.mux)
}

func (s *ReadAPISuite) TearDownTest(c *gc.C) {
	// All callbacks were consumed.
	c.Check(s.readCallbacks, gc.HasLen, 0)
}

func (s *ReadAPISuite) TearDownSuite(c *gc.C) {
	s.cfs.Close()
	os.RemoveAll(s.localDir)
}

func (s *ReadAPISuite) TestNonBlockingSuccess(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Journal, gc.Equals, journal.Name("journal/name"))
			c.Check(op.Offset, gc.Equals, int64(12350))

			op.Result <- journal.ReadResult{
				Offset:     12350,
				WriteHead:  12371,
				RouteToken: "http://foo|http://bar",
				Fragment:   s.spool.Fragment,
			}
		},
		func(op journal.ReadOp) {
			// Second read. Expect the offset reflects the previous read.
			c.Check(op.Offset, gc.Equals, int64(12371))
			// Return an error to break the read loop.
			op.Result <- journal.ReadResult{
				Error:     journal.ErrNotYetAvailable,
				Offset:    12371,
				WriteHead: 12371,
			}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
	c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
		fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))
	c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, "12371")
	c.Check(w.HeaderMap.Get(RouteTokenHeader), gc.Equals, "http://foo|http://bar")
	c.Check(w.HeaderMap.Get(FragmentNameHeader), gc.Equals,
		"0000000000003039-0000000000003053-1c0a8050f4bf53c7846c703b909ff866b1eddbd0")
	c.Check(w.Body.String(), gc.Equals, "expected read fixture")
}

func (s *ReadAPISuite) TestBlockingSuccess(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350&block=true", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			// First probe is non-blocking. Pretend read isn't available.
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Offset, gc.Equals, int64(12350))

			op.Result <- journal.ReadResult{
				Error:     journal.ErrNotYetAvailable,
				Offset:    12350,
				WriteHead: 12350,
			}
		},
		func(op journal.ReadOp) {
			// Second probe is blocking.
			c.Check(op.Blocking, gc.Equals, true)
			c.Check(op.Offset, gc.Equals, int64(12350))

			// Expect headers have already been flushed to the client.
			c.Check(w.Flushed, gc.Equals, true)
			c.Check(w.Code, gc.Equals, http.StatusPartialContent)
			c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
				fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))
			c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, "12350")
			// Fragment information was omitted, as it wasn't available
			// when headers were written.

			op.Result <- journal.ReadResult{
				Offset:    12350,
				WriteHead: 12371,
				Fragment:  s.spool.Fragment,
			}
		},
		func(op journal.ReadOp) {
			// Return a non-local result. Because this is not the first read of the
			// request, we stop here (rather than starting a new proxied transfer).
			op.Result <- journal.ReadResult{
				Offset:    12371,
				WriteHead: 12400,
				Fragment: journal.Fragment{
					Journal: "journal/name",
					Begin:   12350,
					End:     12400,
				},
			}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Body.String(), gc.Equals, "expected read fixture")
}

func (s *ReadAPISuite) TestBlockingTimeout(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350&blockms=10000", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			// First probe is non-blocking. Pretend read isn't available.
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Offset, gc.Equals, int64(12350))
			c.Check(op.Deadline.IsZero(), gc.Equals, true)

			op.Result <- journal.ReadResult{
				Error:     journal.ErrNotYetAvailable,
				Offset:    12350,
				WriteHead: 12350,
				Fragment:  s.spool.Fragment,
			}
		},
		func(op journal.ReadOp) {
			// Check that passing a value for |blockms| was interpreted as a block
			// (journal.Tail will handle the interpretation of a timeout as a subset of a block)
			c.Check(op.Blocking, gc.Equals, true)
			c.Check(op.Offset, gc.Equals, int64(12350))
			// Verify that the op's deadline has been set in the future
			c.Check(op.Deadline.After(time.Now()), gc.Equals, true)

			op.Result <- journal.ReadResult{Error: journal.ErrNotReplica}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
}

func (s *ReadAPISuite) TestBlockingReadFromHead(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=-1&block=true", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			// First probe is non-blocking. Return the current write head.
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Offset, gc.Equals, int64(-1))

			op.Result <- journal.ReadResult{
				Error:     journal.ErrNotYetAvailable,
				Offset:    12350,
				WriteHead: 12350,
			}
		},
		func(op journal.ReadOp) {
			// Second read is blocking, with the previously returned offset.
			c.Check(op.Blocking, gc.Equals, true)
			c.Check(op.Offset, gc.Equals, int64(-1))

			// Expect headers have already been flushed to the client.
			c.Check(w.Flushed, gc.Equals, true)
			c.Check(w.Code, gc.Equals, http.StatusPartialContent)

			c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
				fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))
			c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, "12350")

			op.Result <- journal.ReadResult{
				Offset:    12350,
				WriteHead: 12371,
				Fragment:  s.spool.Fragment,
			}
		},
		func(op journal.ReadOp) {
			// Simulate a cluster change: expect blocking read is server-closed.
			op.Result <- journal.ReadResult{Error: journal.ErrNotReplica}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Body.String(), gc.Equals, "expected read fixture")
}

func (s *ReadAPISuite) TestReadOfEmptyStream(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?block=true", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Journal, gc.Equals, journal.Name("journal/name"))
			c.Check(op.Offset, gc.Equals, int64(0))

			op.Result <- journal.ReadResult{Error: journal.ErrNotYetAvailable}
		},
		func(op journal.ReadOp) {
			c.Check(op.Blocking, gc.Equals, true)
			c.Check(op.Offset, gc.Equals, int64(0))

			c.Check(w.Flushed, gc.Equals, true)
			c.Check(w.Code, gc.Equals, http.StatusPartialContent)
			c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
				fmt.Sprintf("bytes 0-%v/%v", math.MaxInt64, math.MaxInt64))
			c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, "0")

			// Return an error to break the read loop.
			op.Result <- journal.ReadResult{Error: journal.ErrNotReplica}
		},
	}
	s.mux.ServeHTTP(w, req)
}

func (s *ReadAPISuite) TestHEADWithRemoteFragment(c *gc.C) {
	req, _ := http.NewRequest("HEAD", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Offset, gc.Equals, int64(12350))

			op.Result <- journal.ReadResult{
				Offset:    12350,
				WriteHead: 12371,
				Fragment: journal.Fragment{
					Journal: "journal/name",
					Begin:   12350,
					End:     12371,
				},
			}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
	c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
		fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))
	c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, "12371")
	c.Check(w.HeaderMap.Get(FragmentNameHeader), gc.Equals,
		"000000000000303e-0000000000003053-0000000000000000000000000000000000000000")
	c.Check(w.HeaderMap.Get(FragmentLocationHeader), gc.Matches,
		"file:///.*/journal/name/000000000000303e-0000000000003053-"+
			"0000000000000000000000000000000000000000")
	c.Check(w.Body.String(), gc.Equals, "")
}

func (s *ReadAPISuite) TestInvalidArguments(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=zxvf", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)
	c.Check(w.Code, gc.Equals, http.StatusBadRequest)
	c.Check(w.Body.String(), gc.Equals,
		"schema: error converting value for \"offset\"\n")
}

func (s *ReadAPISuite) TestNotReplica(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			op.Result <- journal.ReadResult{
				Error:      journal.ErrNotReplica,
				RouteToken: "http://other|http://yet-another",
			}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusTemporaryRedirect)
	c.Check(w.Header().Get("Location"), gc.Equals, "http://other/journal/name?offset=12350")
	c.Check(w.Header().Get(WriteHeadHeader), gc.Equals, "")
	c.Check(w.Header().Get(RouteTokenHeader), gc.Equals, "http://other|http://yet-another")
}

func (s *ReadAPISuite) TestNotYetAvailable(c *gc.C) {
	req, _ := http.NewRequest("HEAD", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			op.Result <- journal.ReadResult{
				Error:     journal.ErrNotYetAvailable,
				WriteHead: 11223,
			}
		},
	}
	s.mux.ServeHTTP(w, req)

	// Expect X-Write-Head is sent with current write head.
	c.Check(w.Code, gc.Equals, http.StatusRequestedRangeNotSatisfiable)
	c.Check(w.Header().Get(WriteHeadHeader), gc.Equals, "11223")
}

func (s *ReadAPISuite) TestInternalError(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.readCallbacks = []func(journal.ReadOp){
		func(op journal.ReadOp) {
			op.Result <- journal.ReadResult{Error: fmt.Errorf("some error")}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusInternalServerError)
	c.Check(w.Body.String(), gc.Equals, "some error\n")
}

// Implementation of ReadOpHandler.
func (s *ReadAPISuite) Read(op journal.ReadOp) {
	s.readCallbacks[0](op)
	s.readCallbacks = s.readCallbacks[1:]
}

var _ = gc.Suite(&ReadAPISuite{})

func Test(t *testing.T) { gc.TestingT(t) }
