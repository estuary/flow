package protocol

import (
	"time"

	"github.com/gazette/gazette/v2/allocator"
	"github.com/gazette/gazette/v2/keyspace"
	"github.com/gazette/gazette/v2/labels"
	gc "github.com/go-check/check"
	"gopkg.in/yaml.v2"
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
	spec.Labels[0].Value = "" // Label is rejected even if value is empty.
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels cannot include label "name"`)
	spec.Labels[0].Name = "prefix"
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels cannot include label "prefix"`)

	spec.LabelSet = MustLabelSet(labels.MessageType, "a", labels.MessageType, "b")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: expected single-value Label has multiple values .*`)
	spec.LabelSet = MustLabelSet(labels.ContentType, "invalid/content/type")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: parsing `+labels.ContentType+`: mime: unexpected content after media subtype`)
	spec.LabelSet = MustLabelSet(labels.ContentType, "")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: parsing `+labels.ContentType+`: mime: no media type`)
	spec.LabelSet = MustLabelSet(labels.MessageSubType, "subtype")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: expected `+labels.MessageType+` label alongside `+labels.MessageSubType)
	spec.LabelSet = MustLabelSet(labels.MessageSubType, "subtype", labels.MessageType, "type")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: expected `+labels.ContentType+` label alongside `+labels.MessageType)
	spec.LabelSet = MustLabelSet(labels.MessageType, "type", labels.ContentType, "wrong")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: `+labels.ContentType+` label is not a known message framing \(wrong; expected one of map.*`)
	spec.LabelSet = MustLabelSet(labels.MessageType, "type", labels.ContentType, labels.ContentType_RecoveryLog)
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels: `+labels.ContentType+` label is not a known message framing .*`)
	spec.LabelSet = MustLabelSet(labels.MessageType, "type", labels.ContentType, labels.ContentType_JSONLines)
	c.Check(spec.Validate(), gc.IsNil)
	spec.LabelSet = MustLabelSet(labels.MessageSubType, "subtype", labels.MessageType, "type", labels.ContentType, labels.ContentType_JSONLines)
	c.Check(spec.Validate(), gc.IsNil)

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

	f.FlushInterval = time.Minute
	c.Check(f.Validate(), gc.ErrorMatches, `invalid FlushInterval \(1m0s; expected >= 10m0s\)`)
	f.FlushInterval = time.Hour * 2

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

func (s *JournalSuite) TestFlagYAMLRoundTrip(c *gc.C) {
	var cases = []struct {
		Flag JournalSpec_Flag
		enc  string
	}{
		{JournalSpec_O_RDONLY, "flag: O_RDONLY\n"},
		{JournalSpec_O_RDWR, "flag: O_RDWR\n"},
		{JournalSpec_O_RDWR, "flag: O_RDWR\n"},
		{1 | 2 | 32, "flag: 35\n"},
	}
	for _, tc := range cases {
		var b, err = yaml.Marshal(tc)
		c.Check(err, gc.IsNil)
		c.Check(string(b), gc.Equals, tc.enc)

		var f2 = tc
		f2.Flag = 0

		c.Check(yaml.Unmarshal(b, &f2), gc.IsNil)
		c.Check(f2.Flag, gc.Equals, tc.Flag)
	}

	var f JournalSpec_Flag
	c.Check(yaml.Unmarshal([]byte("1"), &f), gc.IsNil)
	c.Check(f, gc.Equals, JournalSpec_O_RDONLY)

	c.Check(yaml.Unmarshal([]byte(`"notAnEnum"`), &f), gc.ErrorMatches,
		`"notAnEnum" is not a valid JournalSpec_Flag \(options are .*\)`)
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
			Stores:           []FragmentStore{"s3://bucket/"},
			RefreshInterval:  time.Minute,
			Retention:        time.Hour,
			FlushInterval:    time.Hour,
		},
		Flags: JournalSpec_O_RDWR,
	}
	var other = JournalSpec{
		Replication: 1,
		LabelSet: LabelSet{
			Labels: []Label{
				{Name: "aaa", Value: "other"},
				{Name: "bbb", Value: "other"},
				{Name: "ccc", Value: "other"},
			},
		},
		Fragment: JournalSpec_Fragment{
			Length:           5678,
			CompressionCodec: CompressionCodec_NONE,
			Stores:           []FragmentStore{"gs://other-bucket/"},
			RefreshInterval:  10 * time.Hour,
			Retention:        10 * time.Hour,
			FlushInterval:    10 * time.Hour,
		},
		Flags: JournalSpec_O_RDONLY,
	}

	c.Check(UnionJournalSpecs(JournalSpec{}, model), gc.DeepEquals, model)
	c.Check(UnionJournalSpecs(model, JournalSpec{}), gc.DeepEquals, model)

	c.Check(UnionJournalSpecs(other, model), gc.DeepEquals, other)
	c.Check(UnionJournalSpecs(model, other), gc.DeepEquals, model)

	c.Check(IntersectJournalSpecs(model, model), gc.DeepEquals, model)
	c.Check(IntersectJournalSpecs(model, JournalSpec{}), gc.DeepEquals, JournalSpec{})
	c.Check(IntersectJournalSpecs(JournalSpec{}, model), gc.DeepEquals, JournalSpec{})

	c.Check(IntersectJournalSpecs(other, model), gc.DeepEquals, JournalSpec{})
	c.Check(IntersectJournalSpecs(model, other), gc.DeepEquals, JournalSpec{})

	c.Check(SubtractJournalSpecs(model, model), gc.DeepEquals, JournalSpec{})
	c.Check(SubtractJournalSpecs(model, JournalSpec{}), gc.DeepEquals, model)
	c.Check(SubtractJournalSpecs(JournalSpec{}, model), gc.DeepEquals, JournalSpec{})

	c.Check(SubtractJournalSpecs(other, model), gc.DeepEquals, other)
	c.Check(SubtractJournalSpecs(model, other), gc.DeepEquals, model)
}

var _ = gc.Suite(&JournalSuite{})
