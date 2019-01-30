package journalspace

import (
	"strings"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

// Node represents a collection of JournalSpecs which are related by
// hierarchical path components, and which may share common portions
// of their journal specifications.
type Node struct {
	// JournalSpec of the Node, which may be partial and incomplete. Specs apply
	// hierarchically, where Nodes having zero-valued fields inherit those of
	// their parent, and parent Nodes may only have a subset of fields specified.
	// A Node is understood to be a "directory" Node if it ends in a slash '/',
	// and a literal or terminal Node otherwise.
	pb.JournalSpec `yaml:",omitempty,inline"`
	// Delete marks that a Node, and all Nodes which it parents, should be deleted.
	Delete bool `yaml:",omitempty"`
	// Revision of the Journal within Etcd. Non-zero for non-directory Nodes only.
	Revision int64 `yaml:",omitempty"`
	// Children of this Node. Directory Nodes must have one or more Children, and
	// non-directory terminal Nodes may not have any.
	Children []Node `yaml:",omitempty"`
}

// IsDir returns whether the Node.JournalSpec.Name is empty or ends in a slash ('/').
func (n Node) IsDir() bool { return len(n.Name) == 0 || n.Name[len(n.Name)-1] == '/' }

// Validate returns an error if the Node hierarchy is not well formed. Note that
// Validate does *not* also Validate contained JournalSpecs, as partial or
// incomplete JournalSpecs are permitted within a Node hierarchy. They must be
// checked separately.
func (n *Node) Validate() error {
	if n.IsDir() {
		if n.Revision != 0 {
			return pb.NewValidationError("unexpected Revision (%d)", n.Revision)
		} else if len(n.Children) == 0 {
			return pb.NewValidationError("expected one or more Children")
		}

		for i, child := range n.Children {
			if !strings.HasPrefix(child.Name.String(), n.Name.String()) {
				return pb.NewValidationError("expected parent Name to prefix child (%s vs %s)",
					n.Name, child.Name)
			} else if i != 0 && n.Children[i-1].Name >= child.Name {
				return pb.NewValidationError("expected Children to be ordered (ind %d; %s vs %s)",
					i, n.Children[i-1].Name, child.Name)
			} else if err := child.Validate(); err != nil {
				return err
			}
		}
		return nil
	}

	if err := pb.Journal(n.Name).Validate(); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if len(n.Children) != 0 {
		return pb.NewValidationError("expected no Children with non-directory Node (%s)", n.Name)
	}
	return nil
}

// Flatten indirect and direct children of the Node into a Node slice,
// merging parent JournalSpecs into those of children at each descending level
// of the hierarchy.
func (n *Node) Flatten() []Node {
	var stack = []Node{*n}
	var out []Node

	for len(stack) != 0 {
		var node Node
		node, stack = stack[len(stack)-1], stack[:len(stack)-1] // Pop.

		if !node.IsDir() {
			out = append(out, node)
			continue
		}

		// Walk Children in reverse order, to preserve forward order on |stack|.
		for i := len(node.Children) - 1; i != -1; i-- {
			var child = node.Children[i]

			if node.Delete {
				child.Delete = true
			}
			child.JournalSpec = pb.UnionJournalSpecs(child.JournalSpec, node.JournalSpec)

			stack = append(stack, child)
		}
	}
	return out
}

// HoistSpecs performs a bottom-up initialization of non-terminal "directory"
// Node JournalSpec fields. Specifically, at each recursive step a parent Node
// specification field is set if the value is shared by all of the Node's
// direct Children. The field is then zeroed at each child in turn. After
// hoisting, the overall repetition of JournalSpec fields in the tree is
// minimized, as every field which is already implied by a parent is zeroed.
func (n *Node) HoistSpecs() {
	// Recursive step: process tree from the bottom, upwards.
	for i := range n.Children {
		if n.Children[i].IsDir() {
			n.Children[i].HoistSpecs()
		}
	}

	// Set n.JournalSpec to the intersection of every child spec.
	var name = n.JournalSpec.Name
	for i, c := range n.Children {
		if i == 0 {
			n.JournalSpec = c.JournalSpec
		} else {
			n.JournalSpec = pb.IntersectJournalSpecs(n.JournalSpec, c.JournalSpec)
		}
	}
	n.JournalSpec.Name = name

	// Subtract portions of child specs covered by the parent spec.
	for i, c := range n.Children {
		n.Children[i].JournalSpec = pb.SubtractJournalSpecs(c.JournalSpec, n.JournalSpec)
	}
}

// ExtractTree derives the tree from ordered []Nodes implied by their shared
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
func ExtractTree(nodes []Node) Node {
	if len(nodes) == 0 {
		return Node{}
	}
	nodes = append([]Node{}, nodes...)

	for len(nodes) > 1 {
		var beg, end, maxLen = 0, 0, -1

		// Find longest run of Nodes having a maximal-length prefix.
		for i := 1; i < len(nodes); i++ {
			if l := sharedPrefix(nodes[i].Name, nodes[i-1].Name); l > maxLen {
				beg, end, maxLen = i-1, i+1, l // |i| begins a new, longest-prefix run.
			} else if l == maxLen && i == end {
				end = i + 1 // |i| extends a current run.
			}
		}
		// Build and splice in a Node to parent and replace nodes[beg:end].
		var node = Node{
			JournalSpec: pb.JournalSpec{Name: nodes[beg].Name[:maxLen]},
			Children:    append([]Node{}, nodes[beg:end]...),
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
