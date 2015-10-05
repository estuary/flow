package gazette

import (
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"os"

	gc "github.com/go-check/check"
	"github.com/gorilla/mux"
)

type ReadAPISuite struct {
	localDir string
	spool    *Spool
	mux      *mux.Router

	readCallbacks      []func(op ReadOp)
	writeHeadCallbacks []func(journal string) int64
}

func (s *ReadAPISuite) SetUpSuite(c *gc.C) {
	// Create a file-backed fragment fixture to return.
	var err error
	s.localDir, err = ioutil.TempDir("", "read-api-suite")
	c.Assert(err, gc.IsNil)

	s.spool, err = NewSpool(s.localDir, "journal/name", 12345)
	c.Check(err, gc.IsNil)

	n, err := s.spool.Write([]byte("XXXXXexpected read fixture"))
	c.Check(err, gc.IsNil)
	c.Check(n, gc.Equals, 26)
	c.Check(s.spool.Commit(int64(n)), gc.IsNil)

	s.mux = mux.NewRouter()
	NewReadAPI(s, nil).Register(s.mux)
}

func (s *ReadAPISuite) TearDownTest(c *gc.C) {
	// All callbacks were consumed.
	c.Check(s.readCallbacks, gc.HasLen, 0)
	c.Check(s.writeHeadCallbacks, gc.HasLen, 0)
}

func (s *ReadAPISuite) TearDownSuite(c *gc.C) {
	os.RemoveAll(s.localDir)
}

func (s *ReadAPISuite) TestGetWriteOffset(c *gc.C) {
	req, _ := http.NewRequest("HEAD", "/journal/name", nil)
	w := httptest.NewRecorder()

	s.writeHeadCallbacks = []func(string) int64{
		func(journal string) int64 {
			c.Check(journal, gc.Equals, "journal/name")
			return 12350
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
	c.Check(w.HeaderMap.Get(WriteHeadHeader), gc.Equals, fmt.Sprintf("12350"))
}

func (s *ReadAPISuite) TestNonBlockingSuccess(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.writeHeadCallbacks = []func(string) int64{
		func(journal string) int64 {
			c.Check(journal, gc.Equals, "journal/name")
			return 12350
		},
	}
	s.readCallbacks = []func(ReadOp){
		func(op ReadOp) {
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Journal, gc.Equals, "journal/name")
			c.Check(op.Offset, gc.Equals, int64(12350))

			op.Result <- ReadResult{Fragment: s.spool.Fragment}
		},
		func(op ReadOp) {
			c.Check(op.Offset, gc.Equals, int64(12371)) // Updated offset.

			op.Result <- ReadResult{Error: ErrNotYetAvailable}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusPartialContent)
	c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
		fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))
	c.Check(w.Body.String(), gc.Equals, "expected read fixture")
}

func (s *ReadAPISuite) TestBlockingSuccess(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350&block=true", nil)
	w := httptest.NewRecorder()

	s.writeHeadCallbacks = []func(string) int64{
		func(journal string) int64 {
			c.Check(journal, gc.Equals, "journal/name")
			return 12350
		},
	}
	s.readCallbacks = []func(ReadOp){
		func(op ReadOp) {
			// First probe is non-blocking. Pretend read isn't available.
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Offset, gc.Equals, int64(12350))

			op.Result <- ReadResult{Error: ErrNotYetAvailable}
		},
		func(op ReadOp) {
			// Second probe is blocking.
			c.Check(op.Blocking, gc.Equals, true)
			c.Check(op.Offset, gc.Equals, int64(12350))

			// Expect headers have already been flushed to the client.
			c.Check(w.Flushed, gc.Equals, true)
			c.Check(w.Code, gc.Equals, http.StatusPartialContent)
			c.Check(w.HeaderMap.Get("Content-Range"), gc.Equals,
				fmt.Sprintf("bytes 12350-%v/%v", math.MaxInt64, math.MaxInt64))

			op.Result <- ReadResult{Fragment: s.spool.Fragment}
		},
		func(op ReadOp) {
			c.Check(op.Offset, gc.Equals, int64(12371))

			// Cluster change: blocking read is server-closed.
			op.Result <- ReadResult{Error: ErrNotReplica}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Body.String(), gc.Equals, "expected read fixture")
}

func (s *ReadAPISuite) TestBlockingReadFromHead(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=-1&block=true", nil)
	w := httptest.NewRecorder()

	s.writeHeadCallbacks = []func(string) int64{
		func(journal string) int64 {
			c.Check(journal, gc.Equals, "journal/name")
			return 12350
		},
	}
	s.readCallbacks = []func(ReadOp){
		func(op ReadOp) {
			c.Check(op.Blocking, gc.Equals, false)
			c.Check(op.Offset, gc.Equals, int64(12350))

			op.Result <- ReadResult{Fragment: s.spool.Fragment}
		},
		func(op ReadOp) {
			c.Check(op.Offset, gc.Equals, int64(12371))

			// Cluster change: blocking read is server-closed.
			op.Result <- ReadResult{Error: ErrNotReplica}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Body.String(), gc.Equals, "expected read fixture")
}

func (s *ReadAPISuite) TestInvalidArguments(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=zxvf&foobar=123", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)
	c.Check(w.Code, gc.Equals, http.StatusBadRequest)
}

func (s *ReadAPISuite) TestNotReplica(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.writeHeadCallbacks = []func(string) int64{
		func(journal string) int64 {
			c.Check(journal, gc.Equals, "journal/name")
			return 12350
		},
	}
	s.readCallbacks = []func(ReadOp){
		func(op ReadOp) {
			op.Result <- ReadResult{Error: ErrNotReplica}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusNotFound)
	c.Check(w.Body.String(), gc.Equals, "not journal replica\n")
}

func (s *ReadAPISuite) TestInternalError(c *gc.C) {
	req, _ := http.NewRequest("GET", "/journal/name?offset=12350", nil)
	w := httptest.NewRecorder()

	s.writeHeadCallbacks = []func(string) int64{
		func(journal string) int64 {
			c.Check(journal, gc.Equals, "journal/name")
			return 12350
		},
	}
	s.readCallbacks = []func(ReadOp){
		func(op ReadOp) {
			op.Result <- ReadResult{Error: ErrWrongRouteToken}
		},
	}
	s.mux.ServeHTTP(w, req)

	c.Check(w.Code, gc.Equals, http.StatusInternalServerError)
	c.Check(w.Body.String(), gc.Equals, "wrong route token\n")
}

// Implementation of headAndReadDispatcher.
func (s *ReadAPISuite) WriteHead(journal string) int64 {
	result := s.writeHeadCallbacks[0](journal)
	s.writeHeadCallbacks = s.writeHeadCallbacks[1:]
	return result
}

func (s *ReadAPISuite) DispatchRead(op ReadOp) {
	s.readCallbacks[0](op)
	s.readCallbacks = s.readCallbacks[1:]
}

var _ = gc.Suite(&ReadAPISuite{})
