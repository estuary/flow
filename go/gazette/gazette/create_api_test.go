package gazette

import (
	"net/http"
	"net/http/httptest"

	etcd "github.com/coreos/etcd/client"
	gc "github.com/go-check/check"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/mock"

	"github.com/pippio/gazette/consensus"
)

type CreateAPISuite struct {
	keys *consensus.MockKeysAPI
	mux  *mux.Router
}

func (s *CreateAPISuite) SetUpTest(c *gc.C) {
	s.keys = new(consensus.MockKeysAPI)
	s.mux = mux.NewRouter()
	NewCreateAPI(s.keys, 2).Register(s.mux)
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
}

func (s *CreateAPISuite) TestCreateFails(c *gc.C) {
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

var _ = gc.Suite(&CreateAPISuite{})
