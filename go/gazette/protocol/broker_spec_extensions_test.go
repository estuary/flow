package protocol

import (
	gc "github.com/go-check/check"
)

type BrokerSpecSuite struct{}

func (s *BrokerSpecSuite) TestIDValidationCases(c *gc.C) {
	var cases = []struct {
		id     ProcessSpec_ID
		expect string
	}{
		{ProcessSpec_ID{Zone: "a-zone", Suffix: "a-name"}, ""}, // Success.
		{ProcessSpec_ID{Zone: "", Suffix: "a-name"}, "Zone: invalid length .*"},
		{ProcessSpec_ID{Zone: "&*", Suffix: "a-name"}, `Zone: not a valid token .*`},
		{ProcessSpec_ID{Zone: "a-very-very-very-very-looong-zone", Suffix: "a-name"}, "Zone: invalid length .*"},
		{ProcessSpec_ID{Zone: "a-zone", Suffix: "ae"}, "Suffix: invalid length .*"},
		{ProcessSpec_ID{Zone: "a-zone", Suffix: "&*($"}, "Suffix: not a valid token .*"},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.id.Validate(), gc.IsNil)
		} else {
			c.Check(tc.id.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *BrokerSpecSuite) TestSpecValidationCases(c *gc.C) {
	var model = BrokerSpec{
		ProcessSpec: ProcessSpec{
			Id:       ProcessSpec_ID{Zone: "a-zone", Suffix: "a-name"},
			Endpoint: "http://foo",
		},
		JournalLimit: 5,
	}
	c.Check(model.Validate(), gc.Equals, nil)
	c.Check(model.ItemLimit(), gc.Equals, 5)

	model.Id.Zone = ""
	c.Check(model.Validate(), gc.ErrorMatches, "Id.Zone: invalid length .*")

	model.Id.Zone = "a-zone"
	model.Endpoint = "invalid"
	c.Check(model.Validate(), gc.ErrorMatches, "Endpoint: not absolute: .*")

	model.Endpoint = "http://foo"
	model.JournalLimit = maxBrokerJournalLimit + 1
	c.Check(model.Validate(), gc.ErrorMatches, `invalid JournalLimit \(\d+; expected 0 <= JournalLimit <= \d+\)`)
}

var _ = gc.Suite(&BrokerSpecSuite{})
