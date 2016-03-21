package consensus

import (
	"sort"

	etcd "github.com/coreos/etcd/client"
)

// Represents an agreed-upon, ordered set of responsible processes for an item.
type Route struct {
	// Etcd ModifiedIndex which this Route reflects. This is not a modification
	// index of the route itself, but rather is an effective assertion that Route
	// is valid as-of |EtcdIndex|.
	EtcdIndex uint64
	// Directory Node of item.
	Item *etcd.Node
	// Entries of |Item.Nodes|, ordered on increasing CreatedIndex. 'Master' is
	// index 0, followed by item replicas. Depending on the target replica count,
	// there may be additional temporary entries which are neither master nor
	// replica (eg, due to a lost allocation race).
	Entries etcd.Nodes
}

// Initializes a new Route from the |response| and |node|.
func NewRoute(response *etcd.Response, node *etcd.Node) Route {
	rt := Route{
		EtcdIndex: response.Index,
		Item:      node,
		Entries:   append(etcd.Nodes{}, node.Nodes...), // Copy, as we'll re-order.
	}
	rt.init()
	return rt
}

// Returns the index of |name| in |rt.Entries|, or -1.
func (rt Route) Index(name string) int {
	prefix := len(rt.Item.Key) + 1
	for ind := range rt.Entries {
		if rt.Entries[ind].Key[prefix:] == name {
			return ind
		}
	}
	return -1
}

// Returns whether all replicas are ready for the item master to hand off item
// responsibility, without causing a violation of the required replica count.
func (rt Route) IsReadyForHandoff(alloc Allocator) bool {
	if wanted := alloc.Replicas() + 1; len(rt.Entries) < wanted {
		return false
	} else {
		for j := 1; j != wanted; j++ {
			if !alloc.ItemIsReadyForPromotion(rt.Entries[j].Value) {
				return false
			}
		}
		return true
	}
}

// Performs a deep-copy of Route.
func (rt Route) Copy() Route {
	return Route{
		EtcdIndex: rt.EtcdIndex,
		Item:      CopyNode(rt.Item),
		Entries:   CopyNodes(rt.Entries),
	}
}

func (rt Route) init() {
	sort.Sort(createdIndexOrder(rt.Entries))
}

// Implements sort.Interface, ordering by increasing CreatedIndex.
type createdIndexOrder []*etcd.Node

func (h createdIndexOrder) Len() int           { return len(h) }
func (h createdIndexOrder) Less(i, j int) bool { return h[i].CreatedIndex < h[j].CreatedIndex }
func (h createdIndexOrder) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
