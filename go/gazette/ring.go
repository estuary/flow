package gazette

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/services/etcd-client"
	"sync"
)

const ConfigRingPath = "/gazette/members"

type RingEntry struct {
	BaseURL string
}

type Ring struct {
	ring []RingEntry
	mu   sync.Mutex
}

func NewRing(service etcdClient.EtcdService) *Ring {
	ring := &Ring{}

	service.Subscribe(ConfigRingPath, ring)
	return ring
}

func (r *Ring) Route(journal string) []RingEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.ring) == 0 {
		return nil
	} else {
		return []RingEntry{r.ring[0]}
	}
}

func (r *Ring) OnEtcdUpdate(response *etcd.Response, tree *etcd.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var ring []RingEntry
	for _, node := range tree.Nodes {
		var entry RingEntry
		if err := json.Unmarshal([]byte(node.Value), &entry); err != nil {
			log.WithField("err", err).Warn("failed to parse RingEntry")
		}
	}
	r.ring = ring
}
