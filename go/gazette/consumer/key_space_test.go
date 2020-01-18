package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.gazette.dev/core/allocator"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
)

func TestShardConsistencyCases(t *testing.T) {
	var status, primaryStatus = new(pc.ReplicaStatus), &pc.ReplicaStatus{Code: pc.ReplicaStatus_PRIMARY}
	var asn = keyspace.KeyValue{Decoded: allocator.Assignment{Slot: 1, AssignmentValue: status}}
	var all = keyspace.KeyValues{asn, {Decoded: allocator.Assignment{Slot: 0, AssignmentValue: primaryStatus}}}

	assert.False(t, ShardIsConsistent(allocator.Item{}, asn, all))
	status.Code = pc.ReplicaStatus_STANDBY
	assert.True(t, ShardIsConsistent(allocator.Item{}, asn, all))
	status.Code = pc.ReplicaStatus_PRIMARY
	assert.True(t, ShardIsConsistent(allocator.Item{}, asn, all))

	// If we're FAILED, we're consistent only if the primary is also.
	status.Code = pc.ReplicaStatus_FAILED
	assert.False(t, ShardIsConsistent(allocator.Item{}, asn, all))
	primaryStatus.Code = pc.ReplicaStatus_FAILED
	assert.True(t, ShardIsConsistent(allocator.Item{}, asn, all))
}
