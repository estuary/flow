package protocol

import (
	"strings"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/labels"
)

type LabelSuite struct{}

func (s *LabelSuite) TestLabelValidationCases(c *gc.C) {
	var cases = []struct {
		name, value string
		expect      string
	}{
		{"a-label", "a-value", ""},   // Success.
		{"a/label", "a%20value", ""}, // Success.
		{"a|label", "a-value", `Name: not a valid token \(a|label\)`},
		{"a-label", "a|value", `Value: not a valid token \(a|value\)`},
		{"a", "a-value", `Name: invalid length \(1; expected 2 <= length <= 64\)`},
		{strings.Repeat("a", maxLabelLen+1), "a-value", `Name: invalid length \(65; expected .*`},
		{"a-label", "", ""}, // Success
		{"a-label", strings.Repeat("a", maxLabelValueLen+1), `Value: invalid length \(1025; expected .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(Label{Name: tc.name, Value: tc.value}.Validate(), gc.IsNil)
		} else {
			c.Check(Label{Name: tc.name, Value: tc.value}.Validate(), gc.ErrorMatches, tc.expect)
		}
	}

	// No repetition of single-valued labels: no error.
	c.Check(ValidateSingleValueLabels(MustLabelSet("aaa", "bar", "aaa", "baz", labels.ContentType, "a/type")), gc.IsNil)
	// Repetition of single-valued label: errors.
	c.Check(ValidateSingleValueLabels(MustLabelSet("aaa", "bar", labels.ContentType, "a/type", labels.ContentType, "other/type")),
		gc.ErrorMatches, `expected single-value Label has multiple values \(index 2; label `+labels.ContentType+` value other/type\)`)
	c.Check(ValidateSingleValueLabels(MustLabelSet(labels.Instance, "a", labels.Instance, "b", "other", "c")),
		gc.ErrorMatches, `expected single-value Label has multiple values \(index 1; label `+labels.Instance+` value b\)`)
}

func (s *LabelSuite) TestSetValidationCases(c *gc.C) {
	var set = LabelSet{
		Labels: []Label{
			{Name: "bad label", Value: "111"},
			{Name: "aaa", Value: "444"},
			{Name: "aaa", Value: "333"},
			{Name: "aaa", Value: "222"},
			{Name: "ccc", Value: ""},
			{Name: "ccc", Value: ""},
			{Name: "ccc", Value: "555"},
		},
	}

	c.Check(set.Validate(), gc.ErrorMatches, `Labels\[0\].Name: not a valid token \(bad label\)`)
	set.Labels[0].Name = "bbb"

	c.Check(set.Validate(), gc.ErrorMatches, `Labels not in unique, sorted order \(index 1; label name aaa <= bbb\)`)
	set.Labels[0], set.Labels[3] = set.Labels[3], set.Labels[0] // Swap bbb=111 & aaa=222.

	c.Check(set.Validate(), gc.ErrorMatches, `Labels not in unique, sorted order \(index 2; label aaa value 333 <= 444\)`)
	set.Labels[1], set.Labels[2] = set.Labels[2], set.Labels[1] // Swap aaa=444 & aaa=333.

	c.Check(set.Validate(), gc.ErrorMatches, `Labels not in unique, sorted order \(index 5; label ccc value  <= \)`)
	set.Labels[5].Value = "444"

	c.Check(set.Validate(), gc.ErrorMatches, `Label has empty & non-empty values \(index 5; label ccc value 444\)`)
	set.Labels[4].Value = "44"

	c.Check(set.Validate(), gc.IsNil)
}

func (s *LabelSuite) TestEqualityCases(c *gc.C) {
	var lhs = MustLabelSet("one", "1", "three", "3")
	var rhs = MustLabelSet("one", "1")

	c.Check(lhs.Equal(&rhs), gc.Equals, false)
	rhs.AddValue("three", "3")
	c.Check(lhs.Equal(&rhs), gc.Equals, true)
	rhs.AddValue("four", "4")
	c.Check(lhs.Equal(&rhs), gc.Equals, false)

	c.Check(lhs.Equal(nil), gc.Equals, false)
}

func (s *LabelSuite) TestAssign(c *gc.C) {
	var lhs = MustLabelSet("name", "value")

	lhs.Assign(nil)
	c.Check(lhs.Labels, gc.HasLen, 0)

	var other = MustLabelSet("one", "", "two", "")

	c.Check(lhs.Equal(&other), gc.Equals, false)
	lhs.Assign(&other)
	c.Check(lhs.Equal(&other), gc.Equals, true)
}

func (s *LabelSuite) TestValuesOfCases(c *gc.C) {
	var set = LabelSet{
		Labels: []Label{
			{Name: "DDDD", Value: "111"},
			{Name: "aaa", Value: "222"},
			{Name: "aaa", Value: "333"},
			{Name: "aaa", Value: "444"},
			{Name: "ccc", Value: ""},
			{Name: "ff", Value: "555"},
		},
	}
	c.Check(set.Validate(), gc.IsNil)
	var cases = []struct {
		name   string
		values []string
		value  string
	}{
		{"aa", nil, ""},
		{"aaa", []string{"222", "333", "444"}, "222"},
		{"bbb", nil, ""},
		{"ccc", []string{""}, ""},
		{"ddd", nil, ""},
		{"DDDD", []string{"111"}, "111"},
		{"eee", nil, ""},
		{"ff", []string{"555"}, "555"},
	}
	for _, tc := range cases {
		if tc.values == nil {
			c.Check(set.ValuesOf(tc.name), gc.IsNil)
		} else {
			c.Check(set.ValuesOf(tc.name), gc.DeepEquals, tc.values)
		}
		c.Check(set.ValueOf(tc.name), gc.Equals, tc.value)
	}
}

func (s *LabelSuite) TestSetAddAndRemove(c *gc.C) {
	var set LabelSet
	set.AddValue("bb", "3") // Insert empty.
	set.AddValue("aa", "0") // Insert at beginning.
	set.AddValue("bb", "1") // Insert in middle.
	set.AddValue("aa", "2") // Insert in middle.
	set.AddValue("bb", "3") // Repeat.
	set.AddValue("bb", "4") // Insert at end.

	c.Check(set, gc.DeepEquals, MustLabelSet("aa", "0", "aa", "2", "bb", "1", "bb", "3", "bb", "4"))

	set.SetValue("bb", "5") // Replace at end.
	set.SetValue("cc", "6") // Insert at end.
	set.SetValue("00", "6") // Insert at beginning.

	c.Check(set, gc.DeepEquals, MustLabelSet("00", "6", "aa", "0", "aa", "2", "bb", "5", "cc", "6"))

	set.SetValue("00", "7") // Replace at beginning
	set.SetValue("aa", "8") // Replace at middle.

	c.Check(set, gc.DeepEquals, MustLabelSet("00", "7", "aa", "8", "bb", "5", "cc", "6"))

	set.AddValue("bb", "9")
	set.AddValue("00", "00")

	set.Remove("bb") // Remove from middle.
	set.Remove("bb") // Repeat. Not found (middle).
	c.Check(set, gc.DeepEquals, MustLabelSet("00", "00", "00", "7", "aa", "8", "cc", "6"))

	set.Remove("0")  // Not found (before beginning).
	set.Remove("00") // Remove from beginning.
	c.Check(set, gc.DeepEquals, MustLabelSet("aa", "8", "cc", "6"))
	set.Remove("dd") // Not found (past end).
	set.Remove("cc") // Remove from end.
	c.Check(set, gc.DeepEquals, MustLabelSet("aa", "8"))

	set.Remove("aa")
	c.Check(set, gc.DeepEquals, LabelSet{Labels: []Label{}})
}

func (s *LabelSuite) TestUnion(c *gc.C) {
	c.Check(UnionLabelSets(MustLabelSet(
		"bbb", "val-1",
		"bbb", "val-2",
		"ccc", "val-3",
	), MustLabelSet(
		"aaa", "val-4",
		"aaa", "val-5",
		"bbb", "val-6",
		"ddd", "val-7",
	), MustLabelSet("label", "buffer")), gc.DeepEquals, MustLabelSet(
		"aaa", "val-4",
		"aaa", "val-5",
		"bbb", "val-1", // LHS's bbb values are kept.
		"bbb", "val-2",
		"ccc", "val-3",
		"ddd", "val-7",
	))
}

func (s *LabelSuite) TestIntersection(c *gc.C) {
	c.Check(IntersectLabelSets(MustLabelSet(
		"aaa", "val", // Matched.
		"bbb", "val-1", // Differs on value.
		"ccc", "val-1",
		"ccc", "val-2", // Value only on LHS.
		"ddd", "val-1", // Matched.
		"ddd", "val-2", // Matched.
		"eee", "val", // Only on LHS.
		"ggg", "val", // Matched.
	), MustLabelSet(
		"aaa", "val",
		"bbb", "val-2",
		"ccc", "val-1",
		"ddd", "val-1",
		"ddd", "val-2",
		"fff", "val", // Only on RHS.
		"ggg", "val",
	), MustLabelSet("label", "buffer")), gc.DeepEquals, MustLabelSet(
		"aaa", "val",
		"ddd", "val-1",
		"ddd", "val-2",
		"ggg", "val",
	))
}

func (s *LabelSuite) TestSubtraction(c *gc.C) {
	c.Check(SubtractLabelSet(MustLabelSet(
		"aaa", "val", // Matched.
		"bbb", "val-1", // Differs on value.
		"ccc", "val-1",
		"ccc", "val-2", // Value only on LHS.
		"ddd", "val-1", // Matched.
		"ddd", "val-2", // Matched.
		"eee", "val", // Only on LHS.
		"ggg", "val", // Matched.
	), MustLabelSet(
		"aaa", "val",
		"bbb", "val-2",
		"ccc", "val-1",
		"ddd", "val-1",
		"ddd", "val-2",
		"fff", "val", // Only on RHS.
		"ggg", "val",
	), MustLabelSet("label", "buffer")), gc.DeepEquals, MustLabelSet(
		"bbb", "val-1",
		"ccc", "val-1",
		"ccc", "val-2",
		"eee", "val", // Only on LHS.
	))
}

func (s *LabelSuite) TestSelectorValidationCases(c *gc.C) {
	var sel = LabelSelector{
		Include: LabelSet{
			Labels: []Label{
				{Name: "include", Value: "a-value"},
			},
		},
		Exclude: LabelSet{
			Labels: []Label{
				{Name: "exclude", Value: "other-value"},
			},
		},
	}
	c.Check(sel.Validate(), gc.IsNil)

	sel.Include.Labels[0].Name = "bad label"
	c.Check(sel.Validate(), gc.ErrorMatches, `Include.Labels\[0\].Name: not a valid token \(bad label\)`)
	sel.Include.Labels[0].Name = "include"

	sel.Exclude.Labels[0].Name = "bad label"
	c.Check(sel.Validate(), gc.ErrorMatches, `Exclude.Labels\[0\].Name: not a valid token \(bad label\)`)
}

func (s *LabelSuite) TestSelectorMatchingCases(c *gc.C) {
	var sel = LabelSelector{
		Include: MustLabelSet(
			"inc-1", "a-val",
			"inc-2", "",
			"inc-3", "val-1",
			"inc-3", "val-2"),
		Exclude: MustLabelSet(
			"exc-1", "",
			"exc-2", "val-3",
			"exc-2", "val-4",
		),
	}

	var cases = []struct {
		set    LabelSet
		expect bool
	}{
		{set: MustLabelSet(), expect: false}, // Not matched.
		{set: MustLabelSet("foo", "bar", "inc-1", "a-val", "inc-2", "any", "inc-3", "val-1"), expect: true}, // Matched.
		{set: MustLabelSet("foo", "bar", "inc-1", "a-val", "inc-2", "foo", "inc-3", "val-1"), expect: true}, // Matched (invariant to inc-2 value).
		{set: MustLabelSet("foo", "bar", "inc-1", "a-val", "inc-2", "any", "inc-3", "val-2"), expect: true}, // Matched (alternate inc-3 value).

		{set: MustLabelSet("foo", "bar", "inc-1", "bad-val", "inc-2", "any", "inc-3", "val-1"), expect: false},   // Not matched (inc-1 not matched).
		{set: MustLabelSet("foo", "bar", "inc-1", "a-val", "inc-3", "val-1"), expect: false},                     // Not matched (inc-2 missing).
		{set: MustLabelSet("foo", "bar", "inc-1", "a-val", "inc-2", "any", "inc-3", "val-other"), expect: false}, // Not matched (inc-3 not matched).

		{set: MustLabelSet("exc-1", "any", "foo", "bar", "inc-1", "a-val", "inc-2", "any", "inc-3", "val-1"), expect: false},   // Not matched (exc-1 matched).
		{set: MustLabelSet("exc-2", "val-4", "foo", "bar", "inc-1", "a-val", "inc-2", "any", "inc-3", "val-1"), expect: false}, // Not matched (exc-2 matched).
		{set: MustLabelSet("exc-2", "val-ok", "foo", "bar", "inc-1", "a-val", "inc-2", "any", "inc-3", "val-1"), expect: true}, // Matched (exc-2 not matched).
	}
	for _, tc := range cases {
		c.Check(sel.Matches(tc.set), gc.Equals, tc.expect)
	}

	// Clear Include to test an exclude-only selector.
	sel.Include.Labels = nil
	c.Check(sel.Matches(MustLabelSet()), gc.Equals, true)
	c.Check(sel.Matches(MustLabelSet("foo", "bar")), gc.Equals, true)
	c.Check(sel.Matches(MustLabelSet("exc-2", "val-ok", "foo", "bar")), gc.Equals, true)
	c.Check(sel.Matches(MustLabelSet("exc-2", "val-3", "foo", "bar")), gc.Equals, false)
	c.Check(sel.Matches(MustLabelSet("exc-1", "any", "foo", "bar")), gc.Equals, false)
}

func (s *LabelSuite) TestOuterJoin(c *gc.C) {
	var lhs, rhs = MustLabelSet(
		"aaa", "l0",
		"aaa", "l1",
		"ccc", "l2",
		"ccc", "l3",
		"ccc", "l4",
		"eee", "l5",
		"eee", "l6",
	), MustLabelSet(
		"bbb", "r0",
		"ccc", "r1",
		"ccc", "r2",
		"ddd", "r3",
	)

	var expect = []labelJoinCursor{
		{lBeg: 0, lEnd: 2, rBeg: 0, rEnd: 0}, // aaa
		{lBeg: 2, lEnd: 2, rBeg: 0, rEnd: 1}, // bbb
		{lBeg: 2, lEnd: 5, rBeg: 1, rEnd: 3}, // ccc
		{lBeg: 5, lEnd: 5, rBeg: 3, rEnd: 4}, // ddd
		{lBeg: 5, lEnd: 7, rBeg: 4, rEnd: 4}, // eee
		{lBeg: 7, lEnd: 7, rBeg: 4, rEnd: 4}, // eee
	}

	var it = labelJoin{
		setL: lhs.Labels, setR: rhs.Labels,
		lenL: len(lhs.Labels), lenR: len(rhs.Labels),
	}
	for {
		var cur, ok = it.next()
		c.Check(cur, gc.DeepEquals, expect[0])
		expect = expect[1:]

		if !ok {
			break
		}
	}
}

func (s *LabelSuite) TestSelectorParsingCases(c *gc.C) {
	var cases = []struct {
		s      string
		expect LabelSelector
	}{
		{
			s:      "foo = bar",
			expect: LabelSelector{Include: MustLabelSet("foo", "bar")},
		},
		{
			s:      "foo != bar",
			expect: LabelSelector{Exclude: MustLabelSet("foo", "bar")},
		},
		{
			s:      "foo ",
			expect: LabelSelector{Include: MustLabelSet("foo", "")},
		},
		{
			s:      " !foo",
			expect: LabelSelector{Exclude: MustLabelSet("foo", "")},
		},
		{
			s:      " foo in (bing, baz,bar)",
			expect: LabelSelector{Include: MustLabelSet("foo", "bar", "foo", "baz", "foo", "bing")},
		},
		{
			s:      " foo not in ( pear, apple )",
			expect: LabelSelector{Exclude: MustLabelSet("foo", "apple", "foo", "pear")},
		},
		{
			s: "foo==bar, baz !=bing ,apple in (fruit, banana)",
			expect: LabelSelector{
				Include: MustLabelSet("apple", "banana", "apple", "fruit", "foo", "bar"),
				Exclude: MustLabelSet("baz", "bing"),
			},
		},
		{
			s: "!foo,baz,bing not in (thing-one, thing-2),!bar,",
			expect: LabelSelector{
				Include: MustLabelSet("baz", ""),
				Exclude: MustLabelSet("bar", "", "bing", "thing-2", "bing", "thing-one", "foo", ""),
			},
		},
		// Label values may include '='.
		{
			s: "foo = ba=ar, baz=bi=ngo,exc!=who=ops",
			expect: LabelSelector{
				Include: MustLabelSet("foo", "ba=ar", "baz", "bi=ngo"),
				Exclude: MustLabelSet("exc", "who=ops"),
			},
		},
		{
			s: "foo in (bi=ng,ba=ar), exc notin (who=ops,oth=er)",
			expect: LabelSelector{
				Include: MustLabelSet("foo", "bi=ng", "foo", "ba=ar"),
				Exclude: MustLabelSet("exc", "who=ops", "exc", "oth=er"),
			},
		},
	}
	for _, tc := range cases {
		var sel, err = ParseLabelSelector(tc.s)
		c.Check(err, gc.IsNil)
		c.Check(sel, gc.DeepEquals, tc.expect)

		// Round-trip through String. Expect to parse the same LabelSelector.
		sel2, err := ParseLabelSelector(sel.String())
		c.Check(err, gc.IsNil)
		c.Check(sel, gc.DeepEquals, sel2)
	}

	// Case: Expect a useful error message on unmatched expression.
	var _, err = ParseLabelSelector("apple,banana err in (bar)")
	c.Check(err, gc.ErrorMatches,
		`could not match "banana err in \(bar\)" to a label selector expression`)

	// Case: Expect a useful error message on unmatched selector set.
	_, err = ParseLabelSelector("apple,banana in (bar,err baz)")
	c.Check(err, gc.ErrorMatches,
		`parsing "banana in \(bar,err baz\)": could not match "err baz" to a label selector set expression`)

	// Case: '=' is not permitted in the label name.
	_, err = ParseLabelSelector("ban=ana in (bar)")
	c.Check(err, gc.ErrorMatches,
		`could not match "ban=ana in \(bar\)" to a label selector expression`)

	// Case: Expect Validate is called.
	_, err = ParseLabelSelector("foo, foo in (bar)")
	c.Check(err, gc.ErrorMatches,
		`Include: Label has empty & non-empty values \(index 1; label foo value bar\)`)
}

var _ = gc.Suite(&LabelSuite{})
