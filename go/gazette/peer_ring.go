package gazette

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/pippio/etcd-client"
	"hash/fnv"
	"sort"
	"sync"
)

type RingEntry struct {
	BaseURL      string
	HashPosition int
}

type Ring struct {
	ring []RingEntry
	mu   sync.Mutex
}

func NewRing(path string, service etcdClient.EtcdService) *Ring {
	ring := &Ring{}

	service.Subscribe(path, ring)
	return ring
}

func (r *Ring) Route(journal string) []RingEntry {
	var pos int
	{
		h := fnv.New32a()
		h.Write([]byte(journal))
		pos = int(h.Sum32())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	sort.Search(len(r.ring), func (i int) bool {
		return r.ring[i] > 


	})


	if len(r.ring) == 0 {
		return nil
	} else {
		return []RingEntry{r.ring[ind]}
	}
}

func (r *Ring) OnEtcdResponse(response *etcd.Response, tree *etcd.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	tree.Nodes
}
