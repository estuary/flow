package protocol

import (
	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	gc "github.com/go-check/check"
)

type HeaderSuite struct{}

func (s *HeaderSuite) TestEtcdValidationCases(c *gc.C) {
	var model = Header_Etcd{
		ClusterId: 0,
		MemberId:  0,
		Revision:  -42,
		RaftTerm:  0,
	}
	c.Check(model.Validate(), gc.ErrorMatches, `invalid ClusterId \(expected != 0\)`)
	model.ClusterId = 12
	c.Check(model.Validate(), gc.ErrorMatches, `invalid MemberId \(expected != 0\)`)
	model.MemberId = 34
	c.Check(model.Validate(), gc.ErrorMatches, `invalid Revision \(-42; expected 0 < revision\)`)
	model.Revision = 56
	c.Check(model.Validate(), gc.ErrorMatches, `invalid RaftTerm \(expected != 0\)`)
	model.RaftTerm = 78

	c.Check(model.Validate(), gc.IsNil)
}

func (s *HeaderSuite) TestEtcdConversion(c *gc.C) {
	c.Check(Header_Etcd{
		ClusterId: 12,
		MemberId:  34,
		Revision:  56,
		RaftTerm:  78,
	}, gc.Equals, FromEtcdResponseHeader(
		etcdserverpb.ResponseHeader{
			ClusterId: 12,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	))
}

func (s *HeaderSuite) TestHeaderValidationCases(c *gc.C) {
	var model = Header{
		ProcessId: ProcessSpec_ID{"no-suffix", ""},
		Route:     Route{Primary: 2, Members: []ProcessSpec_ID{{"zone", "name"}}},
		Etcd: Header_Etcd{
			ClusterId: 0,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
	c.Check(model.Validate(), gc.ErrorMatches, `ProcessId.Suffix: invalid length .*`)
	model.ProcessId = ProcessSpec_ID{"zone", "name"}
	c.Check(model.Validate(), gc.ErrorMatches, `Route: invalid Primary .*`)
	model.Route.Primary = 0
	c.Check(model.Validate(), gc.ErrorMatches, `Etcd: invalid ClusterId .*`)
	model.Etcd.ClusterId = 12

	c.Check(model.Validate(), gc.IsNil)
}

var _ = gc.Suite(&HeaderSuite{})
