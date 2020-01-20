package broker

import (
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/keyspace"
)

// NewKeySpace returns a KeySpace suitable for use with an Allocator.
// It decodes allocator Items as JournalSpec messages, Members as BrokerSpecs,
// and Assignments as Routes.
func NewKeySpace(prefix string) *keyspace.KeySpace {
	return allocator.NewAllocatorKeySpace(prefix, decoder{})
}

// decoder is an instance of allocator.Decoder. It strictly enforces that
// JournalSpec.Name and BrokerSpec.ID match those derived from the Spec's
// Etcd key. Conceptually, the Etcd key is source-of-truth regarding entity
// naming, but it's helpful to carry these identifiers within the specs
// themselves, making them a stand-alone representation and allowing for
// content-addressing of a spec into its appropriate Etcd key. This decoder
// behavior provides an assertion that these identifiers never diverge.
type decoder struct{}

func (d decoder) DecodeItem(id string, raw *mvccpb.KeyValue) (allocator.ItemValue, error) {
	var s = new(pb.JournalSpec)

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	} else if s.Name.String() != id {
		return nil, pb.NewValidationError("JournalSpec Name doesn't match Item ID (%+v vs %+v)", s.Name, id)
	}
	return s, nil
}

func (d decoder) DecodeMember(zone, suffix string, raw *mvccpb.KeyValue) (allocator.MemberValue, error) {
	var s = new(pb.BrokerSpec)

	if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	} else if s.Id.Zone != zone {
		return nil, pb.NewValidationError("BrokerSpec Zone doesn't match Member Zone (%+v vs %+v)", s.Id.Zone, zone)
	} else if s.Id.Suffix != suffix {
		return nil, pb.NewValidationError("BrokerSpec Suffix doesn't match Member Suffix (%+v vs %+v)", s.Id.Suffix, suffix)
	}
	return s, nil
}

func (d decoder) DecodeAssignment(itemID, memberZone, memberSuffix string, slot int, raw *mvccpb.KeyValue) (allocator.AssignmentValue, error) {
	var s = new(pb.Route)
	if len(raw.Value) == 0 {
		pbx.Init(s, nil)
	} else if err := s.Unmarshal(raw.Value); err != nil {
		return nil, err
	} else if err = s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// JournalIsConsistent returns true if all allocator.Assignments of the
// JournalSpec identified by Item advertise the same Route, denoting that
// all replicas of the journal have synchronized.
func JournalIsConsistent(item allocator.Item, _ keyspace.KeyValue, assignments keyspace.KeyValues) bool {
	var rt pb.Route
	pbx.Init(&rt, assignments)

	return JournalRouteMatchesAssignments(rt, assignments)
}

// JournalRouteMatchesAssignments returns true iff the Route is equivalent to the
// Route marshaled with each of the journal's |assignments|.
func JournalRouteMatchesAssignments(rt pb.Route, assignments keyspace.KeyValues) bool {
	for _, a := range assignments {
		if !rt.Equivalent(a.Decoded.(allocator.Assignment).AssignmentValue.(*pb.Route)) {
			return false
		}
	}
	return len(rt.Members) == len(assignments)
}
