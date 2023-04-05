package ops

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// ShardLabeling is a parsed and validated representation of the Flow
// labels which are attached to Gazette ShardSpecs, that are understood
// by the Flow runtime and influence its behavior with respect to the shard.
type ShardLabeling struct {
	// Catalog build identifier which the task uses.
	Build string
	// Network-addressable hostname of this task shard.
	Hostname string
	// Logging level of the task.
	LogLevel Log_Level
	// Ports is a map from port name to the combined configuration
	// for the port. The runtime itself doesn't actually care
	// about the alpn protocol, but it's there for the sake of
	// completeness.
	Ports []pf.NetworkPort `json:",omitempty"`
	// Key and R-Clock range of the shard.
	Range pf.RangeSpec
	// If non-empty, the shard which this task is splitting from.
	SplitSource string `json:",omitempty"`
	// If non-empty, the shard which this task is splitting into.
	SplitTarget string `json:",omitempty"`
	// Name of the shard's task.
	TaskName string
	// Type of this task (capture, derivation, or materialization).
	TaskType TaskType
}

func NewShardRef(labeling ShardLabeling) *ShardRef {
	return &ShardRef{
		Name:        labeling.TaskName,
		Kind:        labeling.TaskType,
		KeyBegin:    fmt.Sprintf("%08x", labeling.Range.KeyBegin),
		RClockBegin: fmt.Sprintf("%08x", labeling.Range.RClockBegin),
	}
}
