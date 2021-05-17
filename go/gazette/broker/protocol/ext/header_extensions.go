package ext

import (
	epb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
)

// FromEtcdResponseHeader converts an etcd ResponseHeader to an equivalent Header_Etcd.
func FromEtcdResponseHeader(h epb.ResponseHeader) pb.Header_Etcd {
	return pb.Header_Etcd{
		ClusterId: h.ClusterId,
		MemberId:  h.MemberId,
		Revision:  h.Revision,
		RaftTerm:  h.RaftTerm,
	}
}

// NewUnroutedHeader returns a Header with its ProcessId and Etcd fields derived
// from the v3_allocator.State, and Route left as zero-valued. It is a helper for
// APIs which do not utilize item resolution but still return Headers (eg, List
// and Update).
func NewUnroutedHeader(s *allocator.State) (hdr pb.Header) {
	defer s.KS.Mu.RUnlock()
	s.KS.Mu.RLock()

	if s.LocalMemberInd != -1 {
		var member = s.Members[s.LocalMemberInd].Decoded.(allocator.Member)
		hdr.ProcessId = pb.ProcessSpec_ID{Zone: member.Zone, Suffix: member.Suffix}
	}
	hdr.Route = pb.Route{Primary: -1}
	hdr.Etcd = FromEtcdResponseHeader(s.KS.Header)
	return
}
