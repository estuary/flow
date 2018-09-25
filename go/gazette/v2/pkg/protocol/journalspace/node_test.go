package journalspace

import (
	"testing"

	pb "github.com/LiveRamp/gazette/pkg/protocol"
	gc "github.com/go-check/check"
)

type NodeSuite struct{}

func (s *NodeSuite) TestValidationCases(c *gc.C) {
	var node Node

	node.Name = "dir/"
	node.Revision = 1
	c.Check(node.Validate(), gc.ErrorMatches, `unexpected Revision \(1\)`)

	node.Revision = 0
	c.Check(node.Validate(), gc.ErrorMatches, `expected one or more Children`)

	node.Children = append(node.Children,
		Node{JournalSpec: pb.JournalSpec{Name: "foo"}},
		Node{JournalSpec: pb.JournalSpec{Name: "bar"}})
	c.Check(node.Validate(), gc.ErrorMatches,
		`expected parent Name to prefix child \(dir/ vs foo\)`)

	node.Children[0].Name = "dir/foo"
	node.Children[1].Name = "dir/bar"
	c.Check(node.Validate(), gc.ErrorMatches,
		`expected Children to be ordered \(ind 1; dir/foo vs dir/bar\)`)

	node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
	c.Check(node.Validate(), gc.IsNil)

	node.Name = "invalid name"
	node.Revision = 123
	c.Check(node.Validate(), gc.ErrorMatches,
		`Name: not base64 alphabet \(invalid name\)`)

	node.Name = "item"
	c.Check(node.Validate(), gc.ErrorMatches,
		`expected no Children with non-directory Node \(item\)`)

	node.Children = nil
	c.Check(node.Validate(), gc.IsNil)
}

func (s *NodeSuite) TestFlatten(c *gc.C) {
	var root = Node{
		JournalSpec: pb.JournalSpec{
			Name:        "root/",
			Replication: 1,
		},
		Children: []Node{
			{JournalSpec: pb.JournalSpec{
				Name:  "root/aaa/",
				Flags: pb.JournalSpec_O_RDWR,
			},
				Children: []Node{
					{JournalSpec: pb.JournalSpec{
						Name:  "root/aaa/111",
						Flags: pb.JournalSpec_O_RDONLY, // Overrides O_RDWR.
					}},
					{JournalSpec: pb.JournalSpec{Name: "root/aaa/222"}},
				}},
			{JournalSpec: pb.JournalSpec{Name: "root/bbb/"},
				Children: []Node{
					{JournalSpec: pb.JournalSpec{
						Name:        "root/bbb/333/",
						Replication: 2, // Overrides Replication: 1
					},
						Children: []Node{
							{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/"},
								Delete: true,
								Children: []Node{
									{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/x"}},
									{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/y"}},
								}},
							{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/z"}}}},
					{JournalSpec: pb.JournalSpec{
						Name:        "root/bbb/444",
						Replication: 3, // Overrides Replication: 1
						Flags:       pb.JournalSpec_O_WRONLY,
					}},
				}}}}

	c.Check(root.Flatten(), gc.DeepEquals, []Node{
		{JournalSpec: pb.JournalSpec{Name: "root/aaa/111", Replication: 1, Flags: pb.JournalSpec_O_RDONLY}},
		{JournalSpec: pb.JournalSpec{Name: "root/aaa/222", Replication: 1, Flags: pb.JournalSpec_O_RDWR}},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/x", Replication: 2}, Delete: true},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/y", Replication: 2}, Delete: true},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/z", Replication: 2}},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/444", Replication: 3, Flags: pb.JournalSpec_O_WRONLY}},
	})
}

func (s *NodeSuite) TestSharedPrefixCases(c *gc.C) {
	var cases = []struct {
		a, b pb.Journal
		l    int
	}{
		{"foo/bar", "foo/baz", 4},
		{"foo/bar/baz", "foo/bar/bing", 8},
		{"foo/bar/baz/1", "foo/bar/baz/2", 12},
		{"foo/bar/baz/match", "foo/bar/baz/match", 12}, // Only components are considered.
		{"foo/bar/", "foo/baz/", 4},
		{"foo/bar/", "foo/bar/", 8},
		{"miss/", "match/", 0},
	}
	for _, tc := range cases {
		c.Check(sharedPrefix(tc.a, tc.b), gc.Equals, tc.l)
		c.Check(sharedPrefix(tc.b, tc.a), gc.Equals, tc.l)
	}
}

func (s *NodeSuite) TestSpecHoisting(c *gc.C) {
	var root = Node{
		JournalSpec: pb.JournalSpec{Name: "node/"},
		Children: []Node{
			{JournalSpec: pb.JournalSpec{
				Name:        "node/aaa",
				Replication: 2,
				Flags:       pb.JournalSpec_O_RDONLY,
				LabelSet:    labelSet("name-1", "val-1"),
			}},
			{JournalSpec: pb.JournalSpec{
				Name:        "node/bbb",
				Replication: 2,
				Flags:       pb.JournalSpec_O_RDONLY,
				LabelSet:    labelSet("name-1", "val-1", "name-2", "val-2"),
			}},
		}}
	root.HoistSpecs()

	c.Check(root, gc.DeepEquals, Node{
		JournalSpec: pb.JournalSpec{
			Name:        "node/",
			Replication: 2,
			Flags:       pb.JournalSpec_O_RDONLY,
			LabelSet:    labelSet("name-1", "val-1"),
		},
		Children: []Node{
			{JournalSpec: pb.JournalSpec{
				Name: "node/aaa",
			}},
			{JournalSpec: pb.JournalSpec{
				Name:     "node/bbb",
				LabelSet: labelSet("name-2", "val-2"),
			}},
		}})
}

func (s *NodeSuite) TestTreeExtraction(c *gc.C) {
	var root = ExtractTree([]Node{
		{JournalSpec: pb.JournalSpec{Name: "root/aaa/000"}},
		{JournalSpec: pb.JournalSpec{Name: "root/aaa/111/foo/i"}},
		{JournalSpec: pb.JournalSpec{Name: "root/aaa/111/foo/j"}},
		{JournalSpec: pb.JournalSpec{Name: "root/aaa/222"}},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/x"}},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/y"}},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/z"}},
		{JournalSpec: pb.JournalSpec{Name: "root/bbb/444"}},
	})

	c.Check(root, gc.DeepEquals, Node{
		JournalSpec: pb.JournalSpec{Name: "root/"},
		Children: []Node{
			{JournalSpec: pb.JournalSpec{Name: "root/aaa/"},
				Children: []Node{
					{JournalSpec: pb.JournalSpec{Name: "root/aaa/000"}},
					// Expect 111/ and foo/ were *not* mapped into separate nodes,
					// since there are no terminals under 111/ not also under foo/
					{JournalSpec: pb.JournalSpec{Name: "root/aaa/111/foo/"},
						Children: []Node{
							{JournalSpec: pb.JournalSpec{Name: "root/aaa/111/foo/i"}},
							{JournalSpec: pb.JournalSpec{Name: "root/aaa/111/foo/j"}},
						}},
					{JournalSpec: pb.JournalSpec{Name: "root/aaa/222"}},
				}},
			{JournalSpec: pb.JournalSpec{Name: "root/bbb/"},
				Children: []Node{
					{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/"},
						Children: []Node{
							{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/"},
								Children: []Node{
									{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/x"}},
									{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/a/y"}},
								}},
							{JournalSpec: pb.JournalSpec{Name: "root/bbb/333/z"}}}},
					{JournalSpec: pb.JournalSpec{Name: "root/bbb/444"}},
				}}},
	})
}

func labelSet(nv ...string) pb.LabelSet {
	var ls pb.LabelSet
	for i := 0; i < len(nv); i += 2 {
		ls.Labels = append(ls.Labels, pb.Label{Name: nv[i], Value: nv[i+1]})
	}
	if err := ls.Validate(); err != nil {
		panic(err.Error())
	}
	return ls
}

var _ = gc.Suite(&NodeSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
