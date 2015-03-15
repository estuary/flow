package gazette

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/api-server/service"
	"os"
	"sync"
	"time"
)

const ConfigRingPath = "/gazette/members"

type RingEntry struct {
	BaseURL string
}

type Ring struct {
	etcdService service.EtcdService

	ring []RingEntry
	mu   sync.Mutex
}

func NewRing(etcdService service.EtcdService) (*Ring, error) {
	ring := &Ring{etcdService: etcdService}

	if err := etcdService.Subscribe(ConfigRingPath, ring); err != nil {
		return nil, err
	}
	return ring, nil
}

func (r *Ring) Announce(url string) (service.EtcdAnnounceCancelChan, error) {
	var key string
	if hostname, err := os.Hostname(); err != nil {
		return nil, err
	} else {
		key = ConfigRingPath + "/" + hostname
	}

	return r.etcdService.Announce(key,
		RingEntry{BaseURL: url}, time.Minute)
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
		ring = append(ring, entry)
	}
	log.Info("updating gazette ring", ring)
	r.ring = ring
}
