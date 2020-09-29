package protocol

import (
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/labels"
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

		// Journals may have a metadata path segment.
		{"foo/bar;baz/bing", ""},
		// Metadata segments must consist of the token.
		{"foo/bar;disallowed#rune", `metadata path segment: not a valid token \(disallowed#rune\)`},
		// If ';' is present, the following segment must be non-empty.
		{"foo/bar;", `metadata path segment: invalid length \(0; expected 1 <= length <= 512\)`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.j.Validate(), gc.IsNil)
		} else {
			c.Check(tc.j.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *JournalSuite) TestJournalMetaSplits(c *gc.C) {
	c.Check(Journal("foo/bar;meta-part").StripMeta(), gc.Equals, Journal("foo/bar"))
	var n, q = Journal("foo/bar;meta-part").SplitMeta()
	c.Check(n, gc.Equals, Journal("foo/bar"))
	c.Check(q, gc.Equals, ";meta-part")

	c.Check(Journal("foo/bar").StripMeta(), gc.Equals, Journal("foo/bar"))
	n, q = Journal("foo/bar").SplitMeta()
	c.Check(n, gc.Equals, Journal("foo/bar"))
	c.Check(q, gc.Equals, "")
}

func (s *JournalSuite) TestSpecValidationCases(c *gc.C) {
	var spec = JournalSpec{
		Name:        "a/journal",
		Replication: 3,
		LabelSet:    MustLabelSet("aaa", "bbb"),
		Fragment: JournalSpec_Fragment{
			Length:              1 << 18,
			CompressionCodec:    CompressionCodec_GZIP,
			Stores:              []FragmentStore{"s3://bucket/path/", "gs://other-bucket/path/"},
			RefreshInterval:     5 * time.Minute,
			Retention:           365 * 24 * time.Hour,
			PathPostfixTemplate: `date={{ .Spool.FirstAppendTime.Format "2006-01-02" }}/hour={{ .Spool.FirstAppendTime.Format "15" }}`,
		},

		Flags:         JournalSpec_O_RDWR,
		MaxAppendRate: 12345,
	}
	c.Check(spec.Validate(), gc.IsNil) // Base case: validates successfully.

	spec.Name = "/bad/name"
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: cannot begin with '/' \(/bad/name\)`)
	spec.Name = "a/journal;disallowed/meta"
	c.Check(spec.Validate(), gc.ErrorMatches, `Name cannot have a metadata path segment \(;disallowed/meta; expected no segment\)`)
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

	spec.MaxAppendRate = -1
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid MaxAppendRate \(-1; expected >= 0\)`)
	spec.MaxAppendRate = 0

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

	f.FlushInterval = time.Second
	c.Check(f.Validate(), gc.ErrorMatches, `invalid FlushInterval \(1s; expected >= 1m0s\)`)
	f.FlushInterval = time.Hour * 2

	f.PathPostfixTemplate = "{{ bad template"
	c.Check(f.Validate(), gc.ErrorMatches, `PathPostfixTemplate: template: postfix:1: .*`)
	f.PathPostfixTemplate = ""
	c.Check(spec.Validate(), gc.IsNil) // Template may be empty.

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
			Length:              1024,
			CompressionCodec:    CompressionCodec_SNAPPY,
			Stores:              []FragmentStore{"s3://bucket/"},
			RefreshInterval:     time.Minute,
			Retention:           time.Hour,
			FlushInterval:       time.Hour,
			PathPostfixTemplate: "{{ .Foo }}",
		},
		Flags:         JournalSpec_O_RDWR,
		MaxAppendRate: 1e3,
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
			Length:              5678,
			CompressionCodec:    CompressionCodec_NONE,
			Stores:              []FragmentStore{"gs://other-bucket/"},
			RefreshInterval:     10 * time.Hour,
			Retention:           10 * time.Hour,
			FlushInterval:       10 * time.Hour,
			PathPostfixTemplate: "{{ .Bar }}",
		},
		Flags:         JournalSpec_O_RDONLY,
		MaxAppendRate: 1e4,
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
