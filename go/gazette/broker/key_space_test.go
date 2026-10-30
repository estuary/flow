package broker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/keyspace"
)

func TestJournalConsistencyCases(t *testing.T) {
	var routes [3]pb.Route
	var assignments keyspace.KeyValues

	for i := range routes {
		routes[i].Primary = 0
		routes[i].Members = []pb.ProcessSpec_ID{
			{Zone: "zone/a", Suffix: "member/1"},
			{Zone: "zone/a", Suffix: "member/3"},
			{Zone: "zone/b", Suffix: "member/2"},
		}
		assignments = append(assignments, keyspace.KeyValue{
			Decoded: allocator.Assignment{
				AssignmentValue: &routes[i],
				MemberSuffix:    routes[0].Members[i].Suffix,
				MemberZone:      routes[0].Members[i].Zone,
				Slot:            i,
			},
		})
	}

	// Case: all assignments match.
	require.True(t, JournalIsConsistent(allocator.Item{}, keyspace.KeyValue{}, assignments))
	// Case: vary indicated primary of Route.
	routes[0].Primary = 1
	require.False(t, JournalIsConsistent(allocator.Item{}, keyspace.KeyValue{}, assignments))
	routes[0].Primary = 0
	// Case: vary members.
	routes[1].Members = append(routes[1].Members, pb.ProcessSpec_ID{Zone: "zone/b", Suffix: "member/4"})
	require.False(t, JournalIsConsistent(allocator.Item{}, keyspace.KeyValue{}, assignments))
}
