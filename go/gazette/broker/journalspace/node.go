// Package journalspace provides mechanisms for mapping a collection of
// JournalSpecs into a minimally-described hierarchical structure, and for
// mapping back again. This is principally useful for tooling over JournalSpecs,
// which must be written to (& read from) Etcd in fully-specified and explicit
// form (a representation which is great for implementors, but rather tedious
// for cluster operators to work with). Tooling can map JournalSpecs into a
// tree, allow the operator to apply edits in that hierarchical space, and then
// flatten resulting changes for storage back to Etcd.
package journalspace

import (
	"sort"
	"strings"

	pb "go.gazette.dev/core/broker/protocol"
)

// Node represents a collection of JournalSpecs which are related by
// hierarchical path components, and which may share common portions
// of their journal specifications.
type Node struct {
	// Comment is a no-op field which allows tooling to generate and pass-through
	// comments in written YAML output.
	Comment string `yaml:",omitempty"`
	// Delete marks that a Node, and all Nodes which it parents, should be deleted.
	Delete *bool `yaml:",omitempty"`
	// JournalSpec of the Node, which may be partial and incomplete. Specs apply
	// hierarchically, where Nodes having zero-valued fields inherit those of
	// their parent, and parent Nodes may only have a subset of fields specified.
	// A Node is understood to be a "directory" Node if it ends in a slash '/',
	// and a literal or terminal Node otherwise.
	Spec pb.JournalSpec `yaml:",omitempty,inline"`
	// Revision of the Journal within Etcd. Non-zero for non-directory Nodes only.
	Revision int64 `yaml:",omitempty"`
	// Children of this Node. Directory Nodes must have one or more Children, and
	// non-directory terminal Nodes may not have any.
	Children []Node `yaml:",omitempty"`

	patched bool
}

// FromListResponse builds a tree from a ListResponse, and returns its root Node.
func FromListResponse(resp *pb.ListResponse) Node {
	// Initialize Nodes for each journal. Extract a journal specification tree,
	// and hoist common configuration to parent nodes to minimize config DRY.
	var nodes []Node

	for i := range resp.Journals {
		nodes = append(nodes, Node{
			Spec:     resp.Journals[i].Spec,
			Revision: resp.Journals[i].ModRevision,
		})
	}

	var tree = extractTree(nodes)
	tree.Hoist()

	return tree
}

// IsDir returns whether the Node.JournalSpec.Name is empty or ends in a slash ('/').
func (n Node) IsDir() bool { return len(n.Spec.Name) == 0 || n.Spec.Name[len(n.Spec.Name)-1] == '/' }

// Validate returns an error if the Node hierarchy is not well formed. Note that
// Validate does *not* also Validate contained JournalSpecs, as partial or
// incomplete JournalSpecs are permitted within a Node hierarchy. They must be
// checked separately.
func (n *Node) Validate() error {
	if n.IsDir() {
		if n.Revision != 0 && n.Revision != -1 {
			return pb.NewValidationError("unexpected Revision (%d)", n.Revision)
		} else if len(n.Children) == 0 {
			return pb.NewValidationError("expected one or more Children")
		}

		for i, child := range n.Children {
			if !strings.HasPrefix(child.Spec.Name.String(), n.Spec.Name.String()) {
				return pb.NewValidationError("expected parent Name to prefix child (%s vs %s)",
					n.Spec.Name, child.Spec.Name)
			} else if i != 0 && n.Children[i-1].Spec.Name >= child.Spec.Name {
				return pb.NewValidationError("expected Children to be ordered (ind %d; %s vs %s)",
					i, n.Children[i-1].Spec.Name, child.Spec.Name)
			} else if err := child.Validate(); err != nil {
				return err
			}
		}
		return nil
	}

	if err := pb.Journal(n.Spec.Name).Validate(); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if len(n.Children) != 0 {
		return pb.NewValidationError("expected no Children with non-directory Node (%s)", n.Spec.Name)
	}
	return nil
}

// Hoist recursively hoists specification values which are common across each of
// a Node's children into the Node itself. Hoisted values are then zeroed at
// each of the Node's children.
func (n *Node) Hoist() {
	// Recursive step: process tree from the bottom, upwards.
	if !n.IsDir() {
		return
	}
	for i := range n.Children {
		n.Children[i].Hoist()
	}

	// Set |n.Spec| to the intersection of every child spec.
	var name = n.Spec.Name
	for i, c := range n.Children {
		if i == 0 {
			n.Delete = c.Delete
			n.Spec = c.Spec
		} else {
			if !deleteEq(n.Delete, c.Delete) {
				n.Delete = nil
			}
			n.Spec = pb.IntersectJournalSpecs(n.Spec, c.Spec)
		}
	}
	n.Spec.Name = name

	// Subtract portions of child specs covered by the parent spec.
	for i, c := range n.Children {
		if n.Delete != nil {
			n.Children[i].Delete = nil
		}
		n.Children[i].Spec = pb.SubtractJournalSpecs(c.Spec, n.Spec)
	}
}

// PushDown specification values from parent Nodes to children, recursively
// copying fields from parent to child where the child field is zero-valued.
// After PushDownSpecs, the hierarchically-implied configuration of each terminal
// made explicit in the representation of that Node.
func (n *Node) PushDown() {
	for i := range n.Children {
		if n.Children[i].Delete == nil {
			n.Children[i].Delete = n.Delete
		}
		n.Children[i].Spec = pb.UnionJournalSpecs(n.Children[i].Spec, n.Spec)
		n.Children[i].PushDown()
	}
	if n.IsDir() {
		// Zero pushed-down parent specification.
		n.Delete, n.Spec = nil, pb.JournalSpec{Name: n.Spec.Name}
	}
}

// WalkTerminalNodes invokes the callback for each of the terminal Nodes of the tree.
// A returned error aborts the recursive walk, and is returned.
func (n *Node) WalkTerminalNodes(cb func(*Node) error) error {
	if !n.IsDir() {
		return cb(n)
	}
	for i := range n.Children {
		if err := n.Children[i].WalkTerminalNodes(cb); err != nil {
			return err
		}
	}
	return nil
}

// Patch |p| into the tree rooted by the Node, inserting it if required and
// otherwise updating with fields of |p| which are not zero-valued. Patch returns
// a reference to the patched *Node, which may also be inspected and updated
// directly. However, note the returned *Node is invalidated with the next call
// to Patch.
func (n *Node) Patch(p Node) *Node {
	if !strings.HasPrefix(p.Spec.Name.String(), n.Spec.Name.String()) {
		// Insert a new tree root with an empty Name prefix. We'll only have
		// to do this once over the life of the tree, since the empty Name
		// prefixes (and roots) all possible Nodes.
		*n = Node{Children: []Node{*n}}
	}

	if !n.IsDir() {
		if n.Spec.Name != p.Spec.Name {
			panic("unexpected Patch of terminal Node")
		}
		if p.Comment != "" {
			n.Comment = p.Comment
		}
		if p.Delete != nil {
			n.Delete = p.Delete
		}
		if p.Revision != 0 {
			n.Revision = p.Revision
		}
		n.Spec = pb.UnionJournalSpecs(p.Spec, n.Spec)
		n.patched = true
		return n
	}

	// Find the first child of |n| which is strictly greater than |p|.
	var ind = sort.Search(len(n.Children), func(i int) bool {
		return n.Children[i].Spec.Name > p.Spec.Name
	})
	// If the Node prior to |ind| is a prefix or exact match, recurse.
	if ind != 0 && strings.HasPrefix(p.Spec.Name.String(), n.Children[ind-1].Spec.Name.String()) {
		return n.Children[ind-1].Patch(p)
	}

	// Node does not yet exist. Splice it in at insertion point |ind|.
	n.Children = append(n.Children, Node{})
	copy(n.Children[ind+1:], n.Children[ind:])
	n.Children[ind] = p

	return n.Children[ind].Patch(p)
}

// MarkUnpatchedForDeletion sets Delete for each terminal Node of the tree
// which has not been Patched.
func (n *Node) MarkUnpatchedForDeletion() {
	for i := range n.Children {
		n.Children[i].MarkUnpatchedForDeletion()
	}
	if !n.IsDir() && !n.patched {
		var boxedTrue = new(bool)
		*boxedTrue = true
		n.Delete = boxedTrue
	}
}

// extractTree derives the tree from ordered []Nodes implied by their shared
// path "directories", or component prefixes. For example, journals:
//  - root/foo/bar
//  - root/foo/baz
//  - root/bing
//
// Will result in the tree:
//  - root/
//    - root/foo/
//      - root/foo/bar
//      - root/foo/baz
//    - root/bing
func extractTree(nodes []Node) Node {
	if len(nodes) == 0 {
		return Node{}
	}
	nodes = append([]Node{}, nodes...)

	for len(nodes) > 1 {
		var beg, end, maxLen = 0, 0, -1

		// Find longest run of Nodes having a maximal-length prefix.
		for i := 1; i < len(nodes); i++ {
			if l := sharedPrefix(nodes[i].Spec.Name, nodes[i-1].Spec.Name); l > maxLen {
				beg, end, maxLen = i-1, i+1, l // |i| begins a new, longest-prefix run.
			} else if l == maxLen && i == end {
				end = i + 1 // |i| extends a current run.
			}
		}
		// Build and splice in a Node to parent and replace nodes[beg:end].
		var node = Node{
			Spec:     pb.JournalSpec{Name: nodes[beg].Spec.Name[:maxLen]},
			Children: append([]Node{}, nodes[beg:end]...),
		}
		nodes = append(nodes[:beg], append([]Node{node}, nodes[end:]...)...)
	}
	return nodes[0]
}

// sharedPrefix returns the number of characters of shared prefix between |a|
// and |b|, evaluated at path component separator boundaries (ie, "/").
func sharedPrefix(a, b pb.Journal) (l int) {
	if len(a) > len(b) {
		a, b = b, a
	}
	for {
		if n := strings.IndexByte(a[l:].String(), '/'); n != -1 && a[:l+n+1] == b[:l+n+1] {
			l += n + 1
		} else {
			return l
		}
	}
}

func deleteEq(a, b *bool) bool {
	if (a == nil) != (b == nil) {
		return false
	} else {
		return a == nil || *a == *b
	}
}
