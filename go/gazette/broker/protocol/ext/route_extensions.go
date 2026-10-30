package ext

import (
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

// Init Initializes Route with the provided allocator Assignments.
func Init(route *pb.Route, assignments keyspace.KeyValues) {
	*route = pb.Route{Primary: -1, Members: route.Members[:0]}

	for _, kv := range assignments {
		var a = kv.Decoded.(allocator.Assignment)
		if a.Slot == 0 {
			route.Primary = int32(len(route.Members))
		}

		route.Members = append(route.Members, pb.ProcessSpec_ID{
			Zone:   a.MemberZone,
			Suffix: a.MemberSuffix,
		})
	}
}

// AttachEndpoints maps Route members through the KeySpace to their respective
// specs, and attaches the associated Endpoint of each to the Route.
// KeySpace must already be read-locked.
func AttachEndpoints(route *pb.Route, ks *keyspace.KeySpace) {
	if len(route.Members) != 0 {
		route.Endpoints = make([]pb.Endpoint, len(route.Members))
	}
	for i, b := range route.Members {
		if member, ok := allocator.LookupMember(ks, b.Zone, b.Suffix); !ok {
			continue // Assignment with missing Member. Ignore.
		} else {
			route.Endpoints[i] = member.MemberValue.(interface {
				GetEndpoint() pb.Endpoint
			}).GetEndpoint()
		}
	}
}
