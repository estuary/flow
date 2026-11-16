package consumer

import (
	"testing"

	"github.com/estuary/flow/go/gazette/allocator"
	pc "github.com/estuary/flow/go/gazette/consumer/protocol"
	"github.com/estuary/flow/go/gazette/keyspace"
	"github.com/stretchr/testify/require"
)

func TestShardConsistencyCases(t *testing.T) {
	var status, primaryStatus = new(pc.ReplicaStatus), &pc.ReplicaStatus{Code: pc.ReplicaStatus_PRIMARY}
	var asn = keyspace.KeyValue{Decoded: allocator.Assignment{Slot: 1, AssignmentValue: status}}
	var all = keyspace.KeyValues{asn, {Decoded: allocator.Assignment{Slot: 0, AssignmentValue: primaryStatus}}}

	require.False(t, ShardIsConsistent(allocator.Item{}, asn, all))
	status.Code = pc.ReplicaStatus_STANDBY
	require.True(t, ShardIsConsistent(allocator.Item{}, asn, all))
	status.Code = pc.ReplicaStatus_PRIMARY
	require.True(t, ShardIsConsistent(allocator.Item{}, asn, all))

	// If we're FAILED, we're consistent only if the primary is also.
	status.Code = pc.ReplicaStatus_FAILED
	require.False(t, ShardIsConsistent(allocator.Item{}, asn, all))
	primaryStatus.Code = pc.ReplicaStatus_FAILED
	require.True(t, ShardIsConsistent(allocator.Item{}, asn, all))
}
