package ext

import (
	gc "github.com/go-check/check"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

type HeaderSuite struct{}

func (s *HeaderSuite) TestEtcdConversion(c *gc.C) {
	c.Check(pb.Header_Etcd{
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
	c.Check(NewUnroutedHeader(fixture), gc.DeepEquals, pb.Header{
		ProcessId: pb.ProcessSpec_ID{Zone: "zone", Suffix: "suffix"},
		Route:     pb.Route{Primary: -1},
		Etcd:      FromEtcdResponseHeader(etcd),
	})
}

var _ = gc.Suite(&HeaderSuite{})
