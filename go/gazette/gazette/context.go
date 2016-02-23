package gazette

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
	"github.com/pippio/gazette/journal"
)

const ServiceRoot = "/gazette/service"

type Context struct {
	CloudFileSystem cloudstore.FileSystem
	Directory       string
	Etcd            discovery.EtcdService
	ReplicaCount    int
	RouteKey        string
	ServiceURL      string

	announceCancel discovery.EtcdAnnounceCancelChan
	router         *Router
	persister      *Persister
}

func (c *Context) Start() error {
	kvs, err := ServiceKeyValueContext(c.Etcd)
	if err != nil {
		return err
	}
	c.announceCancel, err = c.Etcd.Announce(ServiceRoot+"/"+MembersPrefix+c.RouteKey,
		discovery.Endpoint{BaseURL: c.ServiceURL}, time.Minute*10)

	c.router = NewRouter(kvs, c, MembersPrefix+c.RouteKey, c.ReplicaCount)
	c.persister = NewPersister(c.Directory, c.CloudFileSystem, c.Etcd, c.RouteKey).
		StartPersisting()

	for _, fragment := range journal.LocalFragments(c.Directory, "") {
		log.WithField("path", fragment.ContentPath()).Warning("recovering fragment")
		c.persister.Persist(fragment)
	}
	return nil
}

func (c *Context) Stop() {
	c.announceCancel <- struct{}{}
	<-c.announceCancel

	// Wait for the persister to drain its queue.
	c.persister.Stop()
}

func (c *Context) BuildServingMux() http.Handler {
	m := mux.NewRouter()
	NewReadAPI(c.router, c.CloudFileSystem).Register(m)
	NewWriteAPI(c.router).Register(m)
	NewReplicateAPI(c.router).Register(m)
	return m
}

func (c *Context) NewReplica(name journal.Name) JournalReplica {
	return journal.NewReplica(name, c.Directory, c.persister, c.CloudFileSystem)
}

func ServiceKeyValueContext(etcd discovery.EtcdService,
) (*discovery.KeyValueService, error) {
	return discovery.NewKeyValueService(ServiceRoot, etcd, gazetteServiceDecode)
}

func gazetteServiceDecode(key, value string) (interface{}, error) {
	if strings.HasPrefix(key, MembersPrefix) {
		ep := &discovery.Endpoint{}
		err := json.Unmarshal([]byte(value), ep)
		if err == nil {
			_, err = ep.URL() // Validate URL.
		}
		return ep, err
	} else if strings.HasPrefix(key, PersisterLocksPrefix) {
		return value, nil
	} else {
		return nil, errors.New("unknown key type")
	}
}

// Maps common journal errors into a related HTTP status code.
// TODO(johnny): If API handlers are moved to a separate package, move this too.
func ResponseCodeForError(err error) int {
	switch err {
	case journal.ErrNotYetAvailable:
		return http.StatusRequestedRangeNotSatisfiable
	case journal.ErrNotReplica:
		return http.StatusNotFound
	case journal.ErrNotBroker:
		return http.StatusBadGateway
	default:
		// Everything else.
		return http.StatusInternalServerError
	}
}
