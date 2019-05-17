package protocol

import (
	gc "github.com/go-check/check"
	"go.etcd.io/etcd/v3/etcdserver/etcdserverpb"
	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/keyspace"
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
		ProcessId: ProcessSpec_ID{Zone: "no-suffix", Suffix: ""},
		Route:     Route{Primary: 2, Members: []ProcessSpec_ID{{Zone: "zone", Suffix: "name"}}},
		Etcd: Header_Etcd{
			ClusterId: 0,
			MemberId:  34,
			Revision:  56,
			RaftTerm:  78,
		},
	}
	c.Check(model.Validate(), gc.ErrorMatches, `ProcessId.Suffix: invalid length .*`)
	model.ProcessId = ProcessSpec_ID{Zone: "zone", Suffix: "name"}
	c.Check(model.Validate(), gc.ErrorMatches, `Route: invalid Primary .*`)
	model.Route.Primary = 0
	c.Check(model.Validate(), gc.ErrorMatches, `Etcd: invalid ClusterId .*`)
	model.Etcd.ClusterId = 12

	c.Check(model.Validate(), gc.IsNil)

	// Empty ProcessId is permitted.
	model.ProcessId = ProcessSpec_ID{}
	c.Check(model.Validate(), gc.IsNil)
}

func (s *HeaderSuite) TestUnroutedHeader(c *gc.C) {
	var etcd = etcdserverpb.ResponseHeader{
		ClusterId: 12,
		MemberId:  34,
		Revision:  56,
		RaftTerm:  78,
	}
	var fixture = &allocator.State{
		LocalMemberInd: 0,
		Members: keyspace.KeyValues{
			{Decoded: allocator.Member{Zone: "zone", Suffix: "suffix"}},
		},
		KS: &keyspace.KeySpace{
			Header: etcd,
		},
	}
	c.Check(NewUnroutedHeader(fixture), gc.DeepEquals, Header{
		ProcessId: ProcessSpec_ID{Zone: "zone", Suffix: "suffix"},
		Route:     Route{Primary: -1},
		Etcd:      FromEtcdResponseHeader(etcd),
	})
}

var _ = gc.Suite(&HeaderSuite{})
