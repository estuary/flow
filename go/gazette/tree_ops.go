package consensus

import (
	"errors"
	"sort"
	"strings"

	"github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
	etcd "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/store"
)

var (
	EtcdUpsertOps = map[string]struct{}{
		store.CompareAndSwap: {},
		store.Create:         {},
		store.Get:            {},
		store.Set:            {},
		store.Update:         {},
	}
	EtcdRemoveOps = map[string]struct{}{
		store.CompareAndDelete: {},
		store.Delete:           {},
		store.Expire:           {},
	}
)

// Returns whether an etcd.Response.Action is an insert or update.
func isEtcdUpsertOp(action string) bool {
	_, ok := EtcdUpsertOps[action]
	return ok
}

// Returns whether an etcd.Response.Action is a removal.
func isEtcdRemoveOp(action string) bool {
	_, ok := EtcdRemoveOps[action]
	return ok
}

// Precondition: |parent.Nodes| is recursively sorted. Follows the hierarchy
// under |parent| for each successive path element in |names|, and returns the
// *etcd.Node of the leaf element if it exists, or nil otherwise.
func Child(parent *etcd.Node, names ...string) *etcd.Node {
	for len(names) != 0 && parent != nil {
		prefix := len(parent.Key) + 1

		ind := sort.Search(len(parent.Nodes), func(i int) bool {
			return parent.Nodes[i].Key[prefix:] >= names[0]
		})
		if ind < len(parent.Nodes) && parent.Nodes[ind].Key[prefix:] == names[0] {
			parent = parent.Nodes[ind]
			names = names[1:]
		} else {
			parent = nil
		}
	}
	return parent
}

// Precondition: |names| is sorted, as are |parent.Nodes|. For |names| and
// |parent.Nodes|, invokes |cb| with the outer join of a name and its etcd.Node.
func forEachChild(parent *etcd.Node, names []string, cb func(string, *etcd.Node)) {
	nodes := parent.Nodes
	for len(nodes) != 0 || len(names) != 0 {
		// Integrity assertions.
		if len(names) > 1 && names[0] >= names[1] {
			panic("names not ordered")
		}
		if len(nodes) > 1 && nodes[0].Key >= nodes[1].Key {
			panic("nodes not ordered")
		}
		if len(names) != 0 && strings.IndexByte(names[0], '/') != -1 {
			panic("names cannot include slashes")
		}

		var nodeName string
		if len(nodes) != 0 {
			nodeName = nodes[0].Key[len(parent.Key)+1:]
		}

		if len(nodes) == 0 || (len(names) != 0 && nodeName > names[0]) {
			// Current name is not a child of |parent|.
			cb(names[0], &etcd.Node{Key: parent.Key + "/" + names[0]})
			names = names[1:]
		} else if len(names) == 0 || nodeName < names[0] {
			// Current node is not in |names|.
			cb(nodeName, nodes[0])
			nodes = nodes[1:]
		} else {
			// Current name and node match.
			cb(nodeName, nodes[0])
			nodes = nodes[1:]
			names = names[1:]
		}
	}
}

// Precondition: |node| is recursively sorted. Performs a recursive search
// rooted at |node| to identify the |parent| of |key|, and the |index| where
// |key| exists or would be inserted. If a required parent of |key| does not
// exist, its insertion-point is returned.
func FindNode(node *etcd.Node, key string) (parent *etcd.Node, index int) {
	if len(key) <= len(node.Key) {
		return nil, -1
	}
	for {
		var ind int
		var intermediate string

		if slash := strings.IndexByte(key[len(node.Key)+1:], '/'); slash != -1 {
			// Search for the next intermediate directory node.
			intermediate = key[:len(node.Key)+1+slash]
			ind = sort.Search(len(node.Nodes), func(i int) bool {
				return node.Nodes[i].Key >= intermediate
			})
		} else {
			// No directories remain. Look for the exact key.
			ind = sort.Search(len(node.Nodes), func(i int) bool {
				return node.Nodes[i].Key >= key
			})
		}

		if ind < len(node.Nodes) && intermediate == node.Nodes[ind].Key {
			// Node is a parent. Iterate.
			node = node.Nodes[ind]
		} else {
			// |ind| is the node, or is it's insertion point.
			return node, ind
		}
	}
}

// Precondition: |tree| and |response| are recursively sorted. Patches |tree|
// to reflect |response|, returning the new tree root (which may be |tree|).
// PatchTree() can be fed responses of a RetryWatcher() to provide an
// incrementally-built consistent view of a keyspace.
func PatchTree(tree *etcd.Node, response *etcd.Response) (*etcd.Node, error) {
	if response.Action == store.Get && (tree == nil || tree.Key == response.Node.Key) {
		// Init or replace the entire tree.
		return response.Node, nil
	} else if tree == nil {
		return nil, errors.New("not a 'get', and tree is nil")
	} else if !strings.HasPrefix(response.Node.Key, tree.Key) {
		return tree, errors.New("expected response to be a tree subnode")
	}

	parent, ind := FindNode(tree, response.Node.Key)

	// Did we find the exact target node?
	if ind < len(parent.Nodes) && parent.Nodes[ind].Key == response.Node.Key {
		// Indicates an inconsistency.
		if response.Action == store.Create {
			return tree, errors.New("unexpected create")
		}

		if isEtcdUpsertOp(response.Action) {
			parent.Nodes[ind] = response.Node
		} else {
			// Remove the target node.
			if ind+1 < len(parent.Nodes) {
				copy(parent.Nodes[ind:], parent.Nodes[ind+1:])
			}
			parent.Nodes = parent.Nodes[:len(parent.Nodes)-1]
		}
		return tree, nil
	}

	// The target node doesn't exist. A removal op indicates an inconsistency.
	if isEtcdRemoveOp(response.Action) {
		return tree, errors.New("unexpected removal")
	}

	spliceNode := response.Node

	// Build required intermediate directories of the key.
	for subkey := response.Node.Key[len(parent.Key)+1:]; true; {
		if i := strings.LastIndexByte(subkey, '/'); i == -1 {
			break // No intermediate components remain.
		} else {
			subkey = subkey[:i]
		}

		spliceNode = &etcd.Node{
			Key:           parent.Key + "/" + subkey,
			Dir:           true,
			Nodes:         etcd.Nodes{spliceNode},
			CreatedIndex:  spliceNode.CreatedIndex,
			ModifiedIndex: spliceNode.ModifiedIndex,
		}
	}

	// Insert |spliceNode| into |parent| at |ind|.
	parent.Nodes = append(parent.Nodes, spliceNode)
	if ind+1 < len(parent.Nodes) {
		copy(parent.Nodes[ind+1:], parent.Nodes[ind:])
		parent.Nodes[ind] = spliceNode
	}

	return tree, nil
}

// Creates a deep-copy of |node|.
func CopyNode(node *etcd.Node) *etcd.Node {
	result := *node
	result.Nodes = CopyNodes(result.Nodes)
	return &result
}

// Creates a deep-copy of |nodes|.
func CopyNodes(nodes etcd.Nodes) etcd.Nodes {
	if nodes == nil {
		return nil
	}
	result := make(etcd.Nodes, len(nodes))
	for i := range nodes {
		result[i] = CopyNode(nodes[i])
	}
	return result
}

// Returns all non-directory children under |node| via depth-first search
// (eg, maintaining key-order invariants of the tree).
func TerminalNodes(node *etcd.Node) etcd.Nodes {
	if node == nil {
		return []*etcd.Node{}
	} else if !node.Dir {
		return []*etcd.Node{node}
	}

	var result etcd.Nodes
	for _, n := range node.Nodes {
		result = append(result, TerminalNodes(n)...)
	}
	return result
}

// Simple typedef of KeysAPI, presented here for mock generation.
type KeysAPI interface {
	// TODO(johnny): We'd prefer to compose etcd.KeysAPI. However,
	// we rely on mockery for mock generation, and first require a fix for:
	// https://github.com/vektra/mockery/issues/18
	Get(ctx context.Context, key string, opts *etcd.GetOptions) (*etcd.Response, error)
	Set(ctx context.Context, key, value string, opts *etcd.SetOptions) (*etcd.Response, error)
	Delete(ctx context.Context, key string, opts *etcd.DeleteOptions) (*etcd.Response, error)
	Create(ctx context.Context, key, value string) (*etcd.Response, error)
	CreateInOrder(ctx context.Context, dir, value string, opts *etcd.CreateInOrderOptions) (*etcd.Response, error)
	Update(ctx context.Context, key, value string) (*etcd.Response, error)
	Watcher(key string, opts *etcd.WatcherOptions) etcd.Watcher
}
