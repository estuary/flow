package gazette

import (
	"encoding/json"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pippio/api-server/cloudstore"
	"github.com/pippio/api-server/discovery"
	"net/http"
	"strings"
	"time"
)

const GazetteServiceRoot = "/gazette/service"

type ServiceContext struct {
	CloudFileSystem cloudstore.FileSystem
	Directory       string
	Etcd            discovery.EtcdService
	ReplicaCount    int
	RouteKey        string
	ServiceURL      string

	announceCancel discovery.EtcdAnnounceCancelChan
	dispatcher     *dispatcher
	persister      *Persister
}

func (c *ServiceContext) Start() error {
	kvs, err := ServiceKeyValueContext(c.Etcd)
	if err != nil {
		return err
	}
	c.announceCancel, err = c.Etcd.Announce(
		GazetteServiceRoot+"/"+MembersPrefix+c.RouteKey,
		discovery.Endpoint{BaseURL: c.ServiceURL},
		time.Minute*10)

	c.dispatcher = NewDispatcher(kvs, c, MembersPrefix+c.RouteKey, c.ReplicaCount)
	c.persister = NewPersister(c.Directory, c.CloudFileSystem, c.Etcd, c.RouteKey).
		StartPersisting()

	for _, fragment := range LocalFragments(c.Directory, "") {
		log.WithField("path", fragment.ContentPath()).Warning("recovering fragment")
		c.persister.Persist(fragment)
	}
	return nil
}

func (c *ServiceContext) Stop() {
	c.announceCancel <- struct{}{}
	// wait for de-announcement
	<-c.announceCancel
}

func (c *ServiceContext) BuildServingMux() http.Handler {
	m := mux.NewRouter()
	NewReadAPI(c.dispatcher, c.CloudFileSystem).Register(m)
	NewWriteAPI(c.dispatcher).Register(m)
	NewReplicateAPI(c.dispatcher).Register(m)
	return m
}

func (c *ServiceContext) CreateReplica(journal,
	routeToken string) DispatchedJournal {
	j := NewTrackedJournal(journal, c.Directory, c.persister, c.CloudFileSystem)
	j.StartReplicating(routeToken)
	return j
}

func (c *ServiceContext) CreateBroker(journal, routeToken string,
	peers []*discovery.Endpoint) DispatchedJournal {
	j := NewTrackedJournal(journal, c.Directory, c.persister, c.CloudFileSystem)
	j.StartBrokeringWithPeers(routeToken, peers)
	return j
}

func ServiceKeyValueContext(etcd discovery.EtcdService,
) (*discovery.KeyValueService, error) {
	return discovery.NewKeyValueService(GazetteServiceRoot, etcd,
		gazetteServiceDecode)
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
