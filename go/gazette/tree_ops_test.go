package consensus

import (
	"testing"

	etcd "github.com/coreos/etcd/client"
	"github.com/coreos/etcd/store"
	gc "github.com/go-check/check"
)

type TreeOpsSuite struct{}

func (s *TreeOpsSuite) TestChildLookup(c *gc.C) {
	fixture := s.fixture()

	c.Check(Child(fixture, "aa"), gc.Equals, (*etcd.Node)(nil))
	c.Check(Child(fixture, "aaa"), gc.Equals, fixture.Nodes[0])
	c.Check(Child(fixture, "aaa", "0"), gc.Equals, (*etcd.Node)(nil))
	c.Check(Child(fixture, "aaa", "1"), gc.Equals, fixture.Nodes[0].Nodes[0])
	c.Check(Child(fixture, "aaa", "2"), gc.Equals, (*etcd.Node)(nil))
	c.Check(Child(fixture, "aaa", "3"), gc.Equals, fixture.Nodes[0].Nodes[1])
	c.Check(Child(fixture, "aaa", "4"), gc.Equals, (*etcd.Node)(nil))
	c.Check(Child(fixture, "aaaa"), gc.Equals, (*etcd.Node)(nil))
	c.Check(Child(fixture, "bbb"), gc.Equals, fixture.Nodes[1])
	c.Check(Child(fixture, "bbbb"), gc.Equals, (*etcd.Node)(nil))
	c.Check(Child(fixture, "ccc"), gc.Equals, fixture.Nodes[2])
	c.Check(Child(fixture, "ddd"), gc.Equals, (*etcd.Node)(nil))
}

func (s *TreeOpsSuite) TestForEachChildIteration(c *gc.C) {
	fixture := s.fixture()

	var expect map[string]*etcd.Node
	cb := func(name string, node *etcd.Node) {
		c.Check(node.Key, gc.DeepEquals, expect[name].Key)
		c.Check(node.Dir, gc.Equals, expect[name].Dir)
		delete(expect, name)
	}

	expect = map[string]*etcd.Node{
		"aaa": {Key: "/foo/aaa", Dir: true}, // Node before beginning of names.
		"bbb": {Key: "/foo/bbb", Dir: true}, // Node & name match.
		"cc":  {Key: "/foo/cc"},             // Name inserted in middle.
		"ccc": {Key: "/foo/ccc", Dir: true}, // Node after end of names.
	}
	forEachChild(fixture, []string{"bbb", "cc"}, cb)
	c.Check(expect, gc.DeepEquals, map[string]*etcd.Node{})

	expect = map[string]*etcd.Node{
		"aa":   {Key: "/foo/aa"},             // Name before beginning of Nodes.
		"aaa":  {Key: "/foo/aaa", Dir: true}, // Node & name match.
		"bbb":  {Key: "/foo/bbb", Dir: true}, // Node inserted in middle.
		"ccc":  {Key: "/foo/ccc", Dir: true}, // Node & name match.
		"dddd": {Key: "/foo/dddd"},           // Name after end of Nodes.
	}
	forEachChild(fixture, []string{"aa", "aaa", "ccc", "dddd"}, cb)
	c.Check(expect, gc.DeepEquals, map[string]*etcd.Node{})

	c.Check(func() {
		forEachChild(fixture, []string{"wrong", "order"}, cb)
	}, gc.PanicMatches, "names not ordered")

	c.Check(func() {
		forEachChild(fixture, []string{"bad/slash"}, cb)
	}, gc.PanicMatches, "names cannot include slashes")

	fixture.Nodes[0], fixture.Nodes[1] = fixture.Nodes[1], fixture.Nodes[0]
	c.Check(func() {
		forEachChild(fixture, []string{}, cb)
	}, gc.PanicMatches, "nodes not ordered")
}

func (s *TreeOpsSuite) TestFindNode(c *gc.C) {
	fixture := s.fixture()

	parent, ind := FindNode(fixture, "/foo")
	c.Check(parent, gc.IsNil)
	c.Check(ind, gc.Equals, -1)

	parent, ind = FindNode(fixture, "/foo/aaa/0")
	c.Check(parent, gc.Equals, fixture.Nodes[0])
	c.Check(ind, gc.Equals, 0)
	parent, ind = FindNode(fixture, "/foo/aaa/3")
	c.Check(parent, gc.Equals, fixture.Nodes[0])
	c.Check(ind, gc.Equals, 1)
	parent, ind = FindNode(fixture, "/foo/aaa/4")
	c.Check(parent, gc.Equals, fixture.Nodes[0])
	c.Check(ind, gc.Equals, 2)
	parent, ind = FindNode(fixture, "/foo/bb")
	c.Check(parent, gc.Equals, fixture)
	c.Check(ind, gc.Equals, 1)
	parent, ind = FindNode(fixture, "/foo/bbb")
	c.Check(parent, gc.Equals, fixture)
	c.Check(ind, gc.Equals, 1)
	parent, ind = FindNode(fixture, "/foo/bbbb")
	c.Check(parent, gc.Equals, fixture)
	c.Check(ind, gc.Equals, 2)
	parent, ind = FindNode(fixture, "/foo/bbb/ddd/eee/1")
	c.Check(parent, gc.Equals, fixture.Nodes[1])
	c.Check(ind, gc.Equals, 2)
	parent, ind = FindNode(fixture, "/foo/ccc/apple/one")
	c.Check(parent, gc.Equals, fixture.Nodes[2].Nodes[0])
	c.Check(ind, gc.Equals, 0)
	parent, ind = FindNode(fixture, "/foo/ccc/apple/two")
	c.Check(parent, gc.Equals, fixture.Nodes[2].Nodes[0])
	c.Check(ind, gc.Equals, 1)
	parent, ind = FindNode(fixture, "/foo/ccc/banana/two")
	c.Check(parent, gc.Equals, fixture.Nodes[2].Nodes[1])
	c.Check(ind, gc.Equals, 0)
}

func (s *TreeOpsSuite) TestPatchTree(c *gc.C) {
	var err error
	{
		var tree *etcd.Node
		fixture := s.fixture()

		tree, err = PatchTree(tree, &etcd.Response{Node: fixture, Action: store.Get})
		c.Check(err, gc.IsNil)
		c.Check(tree, gc.Equals, fixture)

		// Refresh of tree content.
		tree, err = PatchTree(tree, &etcd.Response{Node: s.fixture(), Action: store.Get})
		c.Check(err, gc.IsNil)

		// Tree was swapped out with new fixture instance.
		c.Check(tree, gc.Not(gc.Equals), fixture)
		c.Check(tree, gc.DeepEquals, fixture)

		// An attempt to patch a sibling tree branch fails.
		tree, err = PatchTree(tree, &etcd.Response{Node: &etcd.Node{Key: "/bar"}, Action: store.Get})
		c.Check(err, gc.ErrorMatches, "expected response to be a tree subnode")
	}
	{
		// Update of a present, nested value.
		tree := s.fixture()

		tree, err = PatchTree(tree, &etcd.Response{
			Node:   &etcd.Node{Key: "/foo/bbb/c", Value: "patched update"},
			Action: store.Set,
		})
		c.Check(err, gc.IsNil)
		c.Check(tree.Nodes[1].Nodes[1].Value, gc.Equals, "patched update")

		tree, err = PatchTree(tree, &etcd.Response{
			Node:   &etcd.Node{Key: "/foo/bbb/c", Value: "patched update"},
			Action: store.Create,
		})
		c.Check(err, gc.ErrorMatches, "unexpected create")
		c.Check(tree, gc.Not(gc.IsNil))

		tree, err = PatchTree(tree, &etcd.Response{
			Node:   &etcd.Node{Key: "/foo/bbb/c", Value: "patched update"},
			Action: "foobar",
		})
		c.Check(err, gc.ErrorMatches, "unknown action")
		c.Check(tree, gc.Not(gc.IsNil))
	}
	{
		// Deletion of a nested value.
		tree := s.fixture()
		c.Check(tree.Nodes[0].Nodes, gc.HasLen, 2) // Precondition.

		tree, err = PatchTree(tree, &etcd.Response{
			Node: &etcd.Node{Key: "/foo/aaa/3", Value: ""}, Action: store.Expire})
		c.Check(err, gc.IsNil)

		c.Check(tree.Nodes[0].Nodes, gc.HasLen, 1)
		c.Check(tree.Nodes[0].Nodes[0].Key, gc.Equals, "/foo/aaa/1")

		_, err = PatchTree(tree, &etcd.Response{
			Node: &etcd.Node{Key: "/foo/aaa/3", Value: ""}, Action: store.Expire})
		c.Check(err, gc.ErrorMatches, "non-upsert of missing key")
		c.Check(tree, gc.Not(gc.IsNil))

		c.Check(tree.Nodes[1].Nodes, gc.HasLen, 2) // Precondition.
		tree, err = PatchTree(tree, &etcd.Response{
			Node: &etcd.Node{Key: "/foo/bbb/a", Value: ""}, Action: store.CompareAndDelete})
		c.Check(err, gc.IsNil)

		c.Check(tree.Nodes[1].Nodes, gc.HasLen, 1)
		c.Check(tree.Nodes[1].Nodes[0].Key, gc.Equals, "/foo/bbb/c")
	}
	{
		// Insertions of new values.
		tree := s.fixture()
		c.Check(tree.Nodes[0].Nodes, gc.HasLen, 2) // Precondition.

		tree, err = PatchTree(tree, &etcd.Response{
			Node: &etcd.Node{Key: "/foo/aaa/0", Value: "zero"}, Action: store.Set})
		c.Check(err, gc.IsNil)
		tree, err = PatchTree(tree, &etcd.Response{
			Node: &etcd.Node{Key: "/foo/aaa/2", Value: "two"}, Action: store.Set})
		c.Check(err, gc.IsNil)
		tree, err = PatchTree(tree, &etcd.Response{
			Node: &etcd.Node{Key: "/foo/aaa/4", Value: "four"}, Action: store.Set})
		c.Check(err, gc.IsNil)

		c.Check(tree.Nodes[0].Nodes, gc.HasLen, 5)
		c.Check(tree.Nodes[0].Nodes[0].Key, gc.Equals, "/foo/aaa/0")
		c.Check(tree.Nodes[0].Nodes[1].Key, gc.Equals, "/foo/aaa/1")
		c.Check(tree.Nodes[0].Nodes[2].Key, gc.Equals, "/foo/aaa/2")
		c.Check(tree.Nodes[0].Nodes[3].Key, gc.Equals, "/foo/aaa/3")
		c.Check(tree.Nodes[0].Nodes[4].Key, gc.Equals, "/foo/aaa/4")
	}
	{
		// Insertion requiring intermediate directories.
		tree := s.fixture()

		apple := tree.Nodes[2].Nodes[0]
		c.Check(apple.Nodes, gc.HasLen, 1) // Precondition.
		tree, err = PatchTree(tree, &etcd.Response{
			Node:   &etcd.Node{Key: "/foo/ccc/apple/on/two/three", Value: "insert"},
			Action: store.Set,
		})
		c.Check(err, gc.IsNil)

		c.Check(apple.Nodes, gc.HasLen, 2)

		c.Check(apple.Nodes[0].Key, gc.Equals, "/foo/ccc/apple/on")
		c.Check(apple.Nodes[0].Dir, gc.Equals, true)
		c.Check(apple.Nodes[1].Key, gc.Equals, "/foo/ccc/apple/one")

		appleOn := apple.Nodes[0]
		c.Check(appleOn.Nodes[0].Key, gc.Equals, "/foo/ccc/apple/on/two")
		c.Check(appleOn.Nodes[0].Dir, gc.Equals, true)

		appleOnTwo := appleOn.Nodes[0]
		c.Check(appleOnTwo.Nodes[0].Key, gc.Equals, "/foo/ccc/apple/on/two/three")
		c.Check(appleOnTwo.Nodes[0].Dir, gc.Equals, false)
		c.Check(appleOnTwo.Nodes[0].Value, gc.Equals, "insert")
	}
	{
		// Deletion of a directory.
		tree := s.fixture()
		c.Check(tree.Nodes, gc.HasLen, 3) // Precondition.

		tree, err = PatchTree(tree, &etcd.Response{
			Node:   &etcd.Node{Key: "/foo/aaa", Value: ""},
			Action: store.Delete,
		})
		c.Check(err, gc.IsNil)

		c.Check(tree.Nodes, gc.HasLen, 2)
		c.Check(tree.Nodes[0].Key, gc.Equals, "/foo/bbb")
		c.Check(tree.Nodes[1].Key, gc.Equals, "/foo/ccc")
	}
}

func (s *TreeOpsSuite) TestDeepCopy(c *gc.C) {
	tree1 := s.fixture()
	tree2 := CopyNode(tree1)

	// Expect trees are deeply equal, but that instances are recursively distinct.
	c.Check(tree1, gc.DeepEquals, tree2)
	c.Check(tree1, gc.Not(gc.Equals), tree2)
	c.Check(tree1.Nodes[0], gc.Not(gc.Equals), tree2.Nodes[0])
	c.Check(tree1.Nodes[0].Nodes[0], gc.Not(gc.Equals), tree2.Nodes[0].Nodes[0])
}

func (s *TreeOpsSuite) TestTerminals(c *gc.C) {
	// Expect it extracts all terminal children in order.
	c.Check(TerminalNodes(s.fixture()), gc.DeepEquals, etcd.Nodes{
		{Key: "/foo/aaa/1"},
		{Key: "/foo/aaa/3", Value: "feed"},
		{Key: "/foo/bbb/a"},
		{Key: "/foo/bbb/c"},
		{Key: "/foo/ccc/apple/one", Value: "beef"},
		{Key: "/foo/ccc/banana"},
	})

	// Terminals of a terminal is itself.
	c.Check(TerminalNodes(&etcd.Node{Key: "/foo/aaa/1"}), gc.DeepEquals,
		etcd.Nodes{{Key: "/foo/aaa/1"}})

	c.Check(TerminalNodes(nil), gc.DeepEquals, etcd.Nodes{})
}

func (s *TreeOpsSuite) TestMapAdapter(c *gc.C) {
	a := MapAdapter(s.fixture())

	c.Check(a.Get(""), gc.Equals, "")
	c.Check(a.Get("aaa/3"), gc.Equals, "feed")
	c.Check(a.Get("bbb/c"), gc.Equals, "")
	c.Check(a.Get("bbb/c/d/e/f"), gc.Equals, "")
	c.Check(a.Get("ccc/apple/one"), gc.Equals, "beef")
	c.Check(a.Get("ccc/apple/two"), gc.Equals, "")
}

func (s *TreeOpsSuite) fixture() *etcd.Node {
	return &etcd.Node{
		Key: "/foo",
		Dir: true,
		Nodes: etcd.Nodes{
			{
				Key:   "/foo/aaa",
				Dir:   true,
				Nodes: etcd.Nodes{{Key: "/foo/aaa/1"}, {Key: "/foo/aaa/3", Value: "feed"}},
			},
			{
				Key:   "/foo/bbb",
				Dir:   true,
				Nodes: etcd.Nodes{{Key: "/foo/bbb/a"}, {Key: "/foo/bbb/c"}},
			},
			{
				Key: "/foo/ccc",
				Dir: true,
				Nodes: etcd.Nodes{
					{
						Key:   "/foo/ccc/apple",
						Dir:   true,
						Nodes: etcd.Nodes{{Key: "/foo/ccc/apple/one", Value: "beef"}},
					},
					{Key: "/foo/ccc/banana"}},
			},
		},
	}
}

var _ = gc.Suite(&TreeOpsSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
