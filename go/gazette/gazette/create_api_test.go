package gazette

import (
	"net/http"
	"net/http/httptest"
	"os"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/mock"

	"github.com/LiveRamp/gazette/cloudstore"
	"github.com/LiveRamp/gazette/consensus"
)

type CreateAPISuite struct {
	keys *consensus.MockKeysAPI
	cfs  cloudstore.FileSystem
	mux  *mux.Router
}

func (s *CreateAPISuite) SetUpTest(c *gc.C) {
	s.keys = new(consensus.MockKeysAPI)
	s.mux = mux.NewRouter()
	s.cfs = cloudstore.NewTmpFileSystem()
	NewCreateAPI(s.cfs, s.keys, 2).Register(s.mux)
}

func (s *CreateAPISuite) TestDownTest(c *gc.C) {
	c.Check(s.cfs.Close(), gc.IsNil)
}

func (s *CreateAPISuite) TestCreateSuccess(c *gc.C) {
	s.keys.On("Set", mock.Anything, ServiceRoot+"/items/journal%2Fname", "",
		&etcd.SetOptions{
			Dir:       true,
			PrevExist: etcd.PrevNoExist}).
		Return(&etcd.Response{Index: 1234}, nil)

	var watcher consensus.MockWatcher

	s.keys.On("Watcher", ServiceRoot+"/items/journal%2Fname",
		&etcd.WatcherOptions{
			AfterIndex: 1234,
			Recursive:  true}).
		Return(&watcher)

	// Return a watch fixture with a sufficient number of replica entries.
	watcher.On("Next", mock.Anything).Return(
		&etcd.Response{
			Action: "get",
			Node: &etcd.Node{Nodes: etcd.Nodes{
				{Value: "ready"}, {Value: "ready"}, {Value: "ready"}}},
		}, nil)

	req, _ := http.NewRequest("POST", "/journal/name", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)
	c.Check(w.Code, gc.Equals, http.StatusCreated)
	s.keys.AssertExpectations(c)

	// Expect a fragment directory was created on the cloud filesystem.
	if dir, err := s.cfs.Open("journal/name"); err != nil {
		c.Assert(err, gc.IsNil)
	} else if info, err := dir.Stat(); err != nil {
		c.Assert(err, gc.IsNil)
	} else {
		c.Check(info.IsDir(), gc.Equals, true)
	}
}

func (s *CreateAPISuite) TestEtcdConflict(c *gc.C) {
	s.keys.On("Set", mock.Anything, ServiceRoot+"/items/journal%2Fname", "",
		&etcd.SetOptions{
			Dir:       true,
			PrevExist: etcd.PrevNoExist}).
		Return(nil, etcd.Error{Code: etcd.ErrorCodeNodeExist})

	req, _ := http.NewRequest("POST", "/journal/name", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)
	c.Check(w.Code, gc.Equals, http.StatusConflict)
	s.keys.AssertExpectations(c)
}

func (s *CreateAPISuite) TestJournalIsAlreadyCFSFile(c *gc.C) {
	var fixture, err = s.cfs.OpenFile("a-file-path",
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	c.Assert(err, gc.IsNil)
	c.Assert(fixture.Close(), gc.IsNil)

	var req, _ = http.NewRequest("POST", "/a-file-path", nil)
	var w = httptest.NewRecorder()

	// Expect a 500 with the underlying cloudstore error is returned.
	s.mux.ServeHTTP(w, req)
	c.Check(w.Code, gc.Equals, http.StatusInternalServerError)
	c.Check(w.Body.String(), gc.Matches, "mkdir .*: not a directory\n")
}

var _ = gc.Suite(&CreateAPISuite{})
