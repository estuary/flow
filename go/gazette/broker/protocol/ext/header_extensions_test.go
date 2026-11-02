package ext

import (
	"github.com/estuary/flow/go/gazette/allocator"
	pb "github.com/estuary/flow/go/gazette/broker/protocol"
	"github.com/estuary/flow/go/gazette/keyspace"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	gc "gopkg.in/check.v1"
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
