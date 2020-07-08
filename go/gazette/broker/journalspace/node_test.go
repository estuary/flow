package journalspace

import (
	"bytes"
	"strings"
	"testing"
	"time"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
	"gopkg.in/yaml.v2"
)

type NodeSuite struct{}

func (s *NodeSuite) TestValidationCases(c *gc.C) {
	var node Node

	node.Spec.Name = "dir/"
	node.Revision = 1
	c.Check(node.Validate(), gc.ErrorMatches, `unexpected Revision \(1\)`)

	node.Revision = 0
	c.Check(node.Validate(), gc.ErrorMatches, `expected one or more Children`)

	node.Revision = -1
	c.Check(node.Validate(), gc.ErrorMatches, `expected one or more Children`)

	node.Children = append(node.Children,
		Node{Spec: pb.JournalSpec{Name: "foo"}},
		Node{Spec: pb.JournalSpec{Name: "bar"}})
	c.Check(node.Validate(), gc.ErrorMatches,
		`expected parent Name to prefix child \(dir/ vs foo\)`)

	node.Children[0].Spec.Name = "dir/foo"
	node.Children[1].Spec.Name = "dir/bar"
	c.Check(node.Validate(), gc.ErrorMatches,
		`expected Children to be ordered \(ind 1; dir/foo vs dir/bar\)`)

	node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
	c.Check(node.Validate(), gc.IsNil)

	node.Spec.Name = "invalid name"
	node.Revision = 123
	c.Check(node.Validate(), gc.ErrorMatches,
		`Name: not a valid token \(invalid name\)`)

	node.Spec.Name = "item"
	c.Check(node.Validate(), gc.ErrorMatches,
		`expected no Children with non-directory Node \(item\)`)

	node.Children = nil
	c.Check(node.Validate(), gc.IsNil)
}

func (s *NodeSuite) TestPushDown(c *gc.C) {
	// Create and flatten two tree fixtures: one with a shared common root, and one without.
	for _, root := range []pb.Journal{"", "shared-root/"} {

		var tree = Node{
			Spec: pb.JournalSpec{
				Name:        root,
				Replication: 1,
			},
			Children: []Node{
				{Spec: pb.JournalSpec{
					Name:  root + "aaa/",
					Flags: pb.JournalSpec_O_RDWR,
				},
					Children: []Node{
						{Spec: pb.JournalSpec{
							Name:  root + "aaa/111",
							Flags: pb.JournalSpec_O_RDONLY, // Overrides O_RDWR.
						}},
						{Spec: pb.JournalSpec{Name: root + "aaa/222"}},
					}},
				{Spec: pb.JournalSpec{Name: root + "bbb/"},
					Children: []Node{
						{Spec: pb.JournalSpec{
							Name:        root + "bbb/333/",
							Replication: 2, // Overrides Replication: 1
						},
							Children: []Node{
								{Spec: pb.JournalSpec{Name: root + "bbb/333/a/"},
									Delete: &boxedTrue,
									Children: []Node{
										{Spec: pb.JournalSpec{Name: root + "bbb/333/a/x"}},
										{Spec: pb.JournalSpec{Name: root + "bbb/333/a/y"}, Delete: &boxedFalse},
									}},
								{Spec: pb.JournalSpec{Name: root + "bbb/333/z"}}}},
						{Spec: pb.JournalSpec{
							Name:        root + "bbb/444",
							Replication: 3, // Overrides Replication: 1
							Flags:       pb.JournalSpec_O_WRONLY,
						}},
					}}}}

		c.Check(tree.Validate(), gc.IsNil)
		tree.PushDown()

		c.Check(flatten(tree), gc.DeepEquals, []Node{
			{Spec: pb.JournalSpec{Name: root + "aaa/111", Replication: 1, Flags: pb.JournalSpec_O_RDONLY}},
			{Spec: pb.JournalSpec{Name: root + "aaa/222", Replication: 1, Flags: pb.JournalSpec_O_RDWR}},
			{Spec: pb.JournalSpec{Name: root + "bbb/333/a/x", Replication: 2}, Delete: &boxedTrue},
			{Spec: pb.JournalSpec{Name: root + "bbb/333/a/y", Replication: 2}, Delete: &boxedFalse},
			{Spec: pb.JournalSpec{Name: root + "bbb/333/z", Replication: 2}},
			{Spec: pb.JournalSpec{Name: root + "bbb/444", Replication: 3, Flags: pb.JournalSpec_O_WRONLY}},
		})
	}
}

func (s *NodeSuite) TestPatchAndDeletionMarking(c *gc.C) {
	var tree = extractTree([]Node{
		{Spec: pb.JournalSpec{Name: "root/aaa/000"}},
		{Spec: pb.JournalSpec{Name: "root/aaa/222"}},
	})

	// Introduce several new Nodes.
	_ = tree.Patch(Node{Spec: pb.JournalSpec{Name: "root/aaa/111"}})
	_ = tree.Patch(Node{Spec: pb.JournalSpec{Name: "root/bbb/ccc"}})
	_ = tree.Patch(Node{Spec: pb.JournalSpec{Name: "ddd"}})

	// Updates of existing Nodes. Update via a mix of fields on the patched
	// Node, and by directly updated the returned *Node.
	tree.Patch(Node{Spec: pb.JournalSpec{Name: "root/aaa/000"}}).Spec.Replication = 3

	tree.Patch(Node{
		Spec:    pb.JournalSpec{Name: "ddd"},
		Comment: "foo",
		Delete:  &boxedFalse,
	}).Revision = 123

	c.Check(tree, gc.DeepEquals, Node{
		Children: []Node{
			{
				Spec:     pb.JournalSpec{Name: "ddd"},
				Comment:  "foo",
				Delete:   &boxedFalse,
				Revision: 123,
				patched:  true,
			},
			{Spec: pb.JournalSpec{Name: "root/aaa/"},
				Children: []Node{
					{Spec: pb.JournalSpec{Name: "root/aaa/000", Replication: 3}, patched: true},
					{Spec: pb.JournalSpec{Name: "root/aaa/111"}, patched: true},
					{Spec: pb.JournalSpec{Name: "root/aaa/222"}},
				}},
			{Spec: pb.JournalSpec{Name: "root/bbb/ccc"}, patched: true},
		},
	})

	// Expect MarkUnpatchedForDeletion deletes terminal Nodes not patched within |tree|.
	tree.MarkUnpatchedForDeletion()

	c.Check(flatten(tree.Children[1]), gc.DeepEquals, []Node{
		{Spec: pb.JournalSpec{Name: "root/aaa/000", Replication: 3}, patched: true},
		{Spec: pb.JournalSpec{Name: "root/aaa/111"}, patched: true},
		{Spec: pb.JournalSpec{Name: "root/aaa/222"}, Delete: &boxedTrue}, // Not visited => marked for deletion.
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
		Spec: pb.JournalSpec{Name: "node/"},
		Children: []Node{
			{
				Spec: pb.JournalSpec{
					Name:        "node/aaa",
					Replication: 2,
					Flags:       pb.JournalSpec_O_RDONLY,
					LabelSet:    pb.MustLabelSet("name-1", "val-1"),
				},
				Delete: &boxedTrue,
			},
			{
				Spec: pb.JournalSpec{
					Name:        "node/bbb",
					Replication: 2,
					Flags:       pb.JournalSpec_O_RDONLY,
					LabelSet:    pb.MustLabelSet("name-1", "val-1", "name-2", "val-2"),
				},
				Delete: &boxedTrue,
			},
		}}
	root.Hoist()

	c.Check(root, gc.DeepEquals, Node{
		Spec: pb.JournalSpec{
			Name:        "node/",
			Replication: 2,
			Flags:       pb.JournalSpec_O_RDONLY,
			LabelSet:    pb.MustLabelSet("name-1", "val-1"),
		},
		Delete: &boxedTrue,
		Children: []Node{
			{Spec: pb.JournalSpec{
				Name: "node/aaa",
			}},
			{Spec: pb.JournalSpec{
				Name:     "node/bbb",
				LabelSet: pb.MustLabelSet("name-2", "val-2"),
			}},
		}})
}

func (s *NodeSuite) TestTreeExtraction(c *gc.C) {
	var root = extractTree([]Node{
		{Spec: pb.JournalSpec{Name: "root/aaa/000"}},
		{Spec: pb.JournalSpec{Name: "root/aaa/111/foo/i"}},
		{Spec: pb.JournalSpec{Name: "root/aaa/111/foo/j"}},
		{Spec: pb.JournalSpec{Name: "root/aaa/222"}},
		{Spec: pb.JournalSpec{Name: "root/bbb/333/a/x"}},
		{Spec: pb.JournalSpec{Name: "root/bbb/333/a/y"}},
		{Spec: pb.JournalSpec{Name: "root/bbb/333/z"}},
		{Spec: pb.JournalSpec{Name: "root/bbb/444"}},
	})

	c.Check(root, gc.DeepEquals, Node{
		Spec: pb.JournalSpec{Name: "root/"},
		Children: []Node{
			{Spec: pb.JournalSpec{Name: "root/aaa/"},
				Children: []Node{
					{Spec: pb.JournalSpec{Name: "root/aaa/000"}},
					// Expect 111/ and foo/ were *not* mapped into separate nodes,
					// since there are no terminals under 111/ not also under foo/
					{Spec: pb.JournalSpec{Name: "root/aaa/111/foo/"},
						Children: []Node{
							{Spec: pb.JournalSpec{Name: "root/aaa/111/foo/i"}},
							{Spec: pb.JournalSpec{Name: "root/aaa/111/foo/j"}},
						}},
					{Spec: pb.JournalSpec{Name: "root/aaa/222"}},
				}},
			{Spec: pb.JournalSpec{Name: "root/bbb/"},
				Children: []Node{
					{Spec: pb.JournalSpec{Name: "root/bbb/333/"},
						Children: []Node{
							{Spec: pb.JournalSpec{Name: "root/bbb/333/a/"},
								Children: []Node{
									{Spec: pb.JournalSpec{Name: "root/bbb/333/a/x"}},
									{Spec: pb.JournalSpec{Name: "root/bbb/333/a/y"}},
								}},
							{Spec: pb.JournalSpec{Name: "root/bbb/333/z"}}}},
					{Spec: pb.JournalSpec{Name: "root/bbb/444"}},
				}}},
	})
}

func (s *NodeSuite) TestYAMLRoundTrip(c *gc.C) {
	const yamlFixture = `
name: foo/
replication: 3
labels:
- name: a-name
  value: a-val
fragment:
  length: 1234
  compression_codec: SNAPPY
  stores:
  - s3://a-bucket/
  - gcs://another-bucket/
  refresh_interval: 1m0s
  retention: 1h0m0s
  flush_interval: 2m0s
  path_postfix_template: foo={{ .Spool.Begin }}
flags: O_RDWR
max_append_rate: 11223344
children:
- delete: true
  name: foo/bar
- name: foo/baz/1
- name: foo/baz/2
  labels:
  - name: other-name
    value: ""
  revision: 1234
`

	var (
		dec  = yaml.NewDecoder(strings.NewReader(yamlFixture))
		tree Node
	)
	dec.SetStrict(true)
	c.Assert(dec.Decode(&tree), gc.IsNil)
	c.Assert(tree.Validate(), gc.IsNil)

	var fragSpec = pb.JournalSpec_Fragment{
		Length:              1234,
		CompressionCodec:    pb.CompressionCodec_SNAPPY,
		Stores:              []pb.FragmentStore{"s3://a-bucket/", "gcs://another-bucket/"},
		RefreshInterval:     time.Minute,
		Retention:           time.Hour,
		FlushInterval:       time.Minute * 2,
		PathPostfixTemplate: "foo={{ .Spool.Begin }}",
	}

	tree.PushDown()
	c.Check(flatten(tree), gc.DeepEquals, []Node{
		{
			Spec: pb.JournalSpec{
				Name:          "foo/bar",
				Replication:   3,
				LabelSet:      pb.MustLabelSet("a-name", "a-val"),
				Fragment:      fragSpec,
				Flags:         pb.JournalSpec_O_RDWR,
				MaxAppendRate: 11223344,
			},
			Delete: &boxedTrue,
		},
		{
			Spec: pb.JournalSpec{
				Name:          "foo/baz/1",
				Replication:   3,
				LabelSet:      pb.MustLabelSet("a-name", "a-val"),
				Fragment:      fragSpec,
				Flags:         pb.JournalSpec_O_RDWR,
				MaxAppendRate: 11223344,
			},
		},
		{
			Spec: pb.JournalSpec{
				Name:          "foo/baz/2",
				Replication:   3,
				LabelSet:      pb.MustLabelSet("a-name", "a-val", "other-name", ""),
				Fragment:      fragSpec,
				Flags:         pb.JournalSpec_O_RDWR,
				MaxAppendRate: 11223344,
			},
			Revision: 1234,
		},
	})
	tree.Hoist()

	var (
		bw  bytes.Buffer
		enc = yaml.NewEncoder(&bw)
	)
	c.Check(enc.Encode(&tree), gc.IsNil)

	c.Check(bw.String(), gc.Equals, yamlFixture[1:])
}

func flatten(tree Node) []Node {
	var out []Node
	_ = tree.WalkTerminalNodes(func(node *Node) error {
		out = append(out, *node)
		return nil
	})
	return out
}

var (
	_          = gc.Suite(&NodeSuite{})
	boxedTrue  = true
	boxedFalse = false
)

func Test(t *testing.T) { gc.TestingT(t) }
