package protocol

import (
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	gc "github.com/go-check/check"
)

type JournalSuite struct{}

func (s *JournalSuite) TestJournalValidationCases(c *gc.C) {
	var cases = []struct {
		j      Journal
		expect string
	}{
		{"a/valid/path/to/a/journal", ""}, // Success.
		{"/leading/slash", `cannot begin with '/' \(/leading/slash\)`},
		{"trailing/slash/", `must be a clean path \(trailing/slash/\)`},
		{"extra-middle//slash", `must be a clean path \(extra-middle//slash\)`},
		{"not-$%|-a valid token", `not a valid token \(.*\)`},
		{"", `invalid length \(0; expected 4 <= .*`},
		{"zz", `invalid length \(2; expected 4 <= .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.j.Validate(), gc.IsNil)
		} else {
			c.Check(tc.j.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *JournalSuite) TestSpecValidationCases(c *gc.C) {
	var spec = JournalSpec{
		Name:        "a/journal",
		Replication: 3,
		LabelSet:    MustLabelSet("aaa", "bbb"),
		Fragment: JournalSpec_Fragment{
			Length:           1 << 18,
			CompressionCodec: CompressionCodec_GZIP,
			Stores:           []FragmentStore{"s3://bucket/path/", "gs://other-bucket/path/"},
			RefreshInterval:  5 * time.Minute,
			Retention:        365 * 24 * time.Hour,
		},

		Flags: JournalSpec_O_RDWR,
	}
	c.Check(spec.Validate(), gc.IsNil) // Base case: validates successfully.

	spec.Name = "/bad/name"
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: cannot begin with '/' \(/bad/name\)`)
	spec.Name = "a/journal"

	spec.Replication = 0
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Replication \(0; expected 1 <= Replication <= 5\)`)
	spec.Replication = 1024
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Replication \(1024; .*`)
	spec.Replication = 3

	spec.Labels[0].Name = "xxx xxx"
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels.Labels\[0\].Name: not a valid token \(xxx xxx\)`)

	spec.Labels[0].Name = "name"
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels cannot include label "name"`)
	spec.Labels[0].Name = "prefix"
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels cannot include label "prefix"`)
	spec.Labels[0].Name = "framing"
	c.Check(spec.Validate(), gc.ErrorMatches, `Label "framing" contains an invalid value \(bbb\)`)
	spec.Labels[0].Value = FramingFixed
	spec.Labels = append(spec.Labels, Label{Name: "framing", Value: FramingJSON})
	c.Check(spec.Validate(), gc.ErrorMatches, `Label "framing" cannot have multiple values`)
	spec.Labels = spec.Labels[:1]

	spec.Fragment.Length = 0
	c.Check(spec.Validate(), gc.ErrorMatches, `Fragment: invalid Length \(0; expected 1024 <= length <= \d+\)`)
	spec.Fragment.Length = 4096

	// Flag combinations which are not permitted.
	for _, f := range []JournalSpec_Flag{
		JournalSpec_O_RDWR | JournalSpec_O_RDONLY,
		JournalSpec_O_RDWR | JournalSpec_O_WRONLY,
		JournalSpec_O_RDONLY | JournalSpec_O_WRONLY,
	} {
		spec.Flags = f
		c.Check(spec.Validate(), gc.ErrorMatches, `Flags: invalid combination \(\d\)`) // Valid alternate Flag value.
	}
	// Permitted flag combinations.
	for _, f := range []JournalSpec_Flag{
		JournalSpec_NOT_SPECIFIED,
		JournalSpec_O_RDWR,
		JournalSpec_O_RDONLY,
		JournalSpec_O_WRONLY,
	} {
		spec.Flags = f
		c.Check(spec.Validate(), gc.IsNil)
	}

	// Additional tests of JournalSpec_Fragment cases.
	var f = &spec.Fragment

	f.Length = maxFragmentLen + 1
	c.Check(f.Validate(), gc.ErrorMatches, `invalid Length \(\d+; expected 1024 <= length <= \d+\)`)
	f.Length = 1024

	f.CompressionCodec = 9999
	c.Check(f.Validate(), gc.ErrorMatches, `CompressionCodec: invalid value \(9999\)`)
	f.CompressionCodec = CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION

	f.Stores[0] = "file:///a/root/"
	c.Check(f.Validate(), gc.ErrorMatches,
		`GZIP_OFFLOAD_DECOMPRESSION is incompatible with file:// stores \(file:///a/root/\)`)
	f.CompressionCodec = CompressionCodec_SNAPPY

	f.RefreshInterval = time.Millisecond
	c.Check(f.Validate(), gc.ErrorMatches, `invalid RefreshInterval \(1ms; expected 1s <= interval <= 24h0m0s\)`)
	f.RefreshInterval = 25 * time.Hour
	c.Check(f.Validate(), gc.ErrorMatches, `invalid RefreshInterval \(25h0m0s; expected 1s <= interval <= 24h0m0s\)`)
	f.RefreshInterval = time.Hour

	f.Stores = append(f.Stores, "invalid")
	c.Check(f.Validate(), gc.ErrorMatches, `Stores\[2\]: not absolute \(invalid\)`)
}

func (s *JournalSuite) TestMetaLabelExtraction(c *gc.C) {
	c.Check(ExtractJournalSpecMetaLabels(&JournalSpec{Name: "path/to/my/journal"}, MustLabelSet("label", "buffer")),
		gc.DeepEquals, MustLabelSet(
			"name", "path/to/my/journal",
			"prefix", "path/",
			"prefix", "path/to/",
			"prefix", "path/to/my/",
		))
}

func (s *JournalSuite) TestConsistencyCases(c *gc.C) {
	var routes [3]Route
	var assignments keyspace.KeyValues

	for i := range routes {
		routes[i].Primary = 0
		routes[i].Members = []ProcessSpec_ID{
			{Zone: "zone/a", Suffix: "member/1"},
			{Zone: "zone/a", Suffix: "member/3"},
			{Zone: "zone/b", Suffix: "member/2"},
		}
		assignments = append(assignments, keyspace.KeyValue{
			Decoded: allocator.Assignment{
				AssignmentValue: &routes[i],
				MemberSuffix:    routes[0].Members[i].Suffix,
				MemberZone:      routes[0].Members[i].Zone,
				Slot:            i,
			},
		})
	}

	var spec JournalSpec
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, true)

	routes[0].Primary = 1
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, false)
	routes[0].Primary = 0

	routes[1].Members = append(routes[1].Members, ProcessSpec_ID{Zone: "zone/b", Suffix: "member/4"})
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, false)

	spec.Flags = JournalSpec_O_RDONLY // Read-only journals are always consistent.
	c.Check(spec.IsConsistent(keyspace.KeyValue{}, assignments), gc.Equals, true)
}

func (s *JournalSuite) TestSetOperations(c *gc.C) {
	var model = JournalSpec{
		Replication: 3,
		LabelSet: LabelSet{
			Labels: []Label{
				{Name: "aaa", Value: "val"},
				{Name: "bbb", Value: "val"},
				{Name: "ccc", Value: "val"},
			},
		},
		Fragment: JournalSpec_Fragment{
			Length:           1024,
			CompressionCodec: CompressionCodec_SNAPPY,
			Stores: []FragmentStore{
				"s3://bucket",
			},
			RefreshInterval: time.Minute,
			Retention:       time.Hour,
		},
		Flags: JournalSpec_O_RDWR,
	}

	c.Check(UnionJournalSpecs(JournalSpec{}, model), gc.DeepEquals, model)
	c.Check(UnionJournalSpecs(model, JournalSpec{}), gc.DeepEquals, model)

	c.Check(IntersectJournalSpecs(model, model), gc.DeepEquals, model)
	c.Check(IntersectJournalSpecs(model, JournalSpec{}), gc.DeepEquals, JournalSpec{})
	c.Check(IntersectJournalSpecs(JournalSpec{}, model), gc.DeepEquals, JournalSpec{})

	c.Check(SubtractJournalSpecs(model, model), gc.DeepEquals, JournalSpec{})
	c.Check(SubtractJournalSpecs(model, JournalSpec{}), gc.DeepEquals, model)
	c.Check(SubtractJournalSpecs(JournalSpec{}, model), gc.DeepEquals, JournalSpec{})
}

var _ = gc.Suite(&JournalSuite{})
