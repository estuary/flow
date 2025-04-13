package labels

import (
	"strings"

	gazLabels "go.gazette.dev/core/labels"
)

// Heads up! Constants in this file must be mirrored to
// crates/protocol/src/labels.rs

// JournalSpec & ShardSpec labels.
const (
	// Build identifies an associated catalog build of the journal or shard.
	Build = "estuary.dev/build"
	// Collection is the name of the Estuary collection for which this Journal
	// holds documents.
	Collection = "estuary.dev/collection"
	// Cordon is a label that indicates that this journal or shard is cordoned
	// due to an ongoing data-plane migration.
	Cordon = "estuary.dev/cordon"
	// Field is a logical partition of the Collection that's implemented by this
	// journal.
	FieldPrefix = "estuary.dev/field/"
	// KeyBegin is a hexadecimal encoding of the beginning key range (inclusive)
	// managed by this journal or shard, in an order-preserving packed []byte embedding.
	KeyBegin = "estuary.dev/key-begin"
	// KeyBeginMin is the minimum possible key.
	KeyBeginMin = "00000000"
	// KeyEnd is a hexadecimal encoding of the ending key range (inclusive)
	// managed by this journal or shard, in an order-preserving packed []byte embedding.
	KeyEnd = "estuary.dev/key-end"
	// KeyEndMax is the maximum possible key.
	KeyEndMax = "ffffffff"
	// ManagedByFlow is a value for the Gazette labels.ManagedBy label.
	ManagedByFlow = "estuary.dev/flow"
)

// ShardSpec labels.
const (
	// TaskName of this shard within the catalog.
	TaskName = "estuary.dev/task-name"
	// TaskType of this shard's task.
	// This is implied by the associated catalog task, and is informational.
	TaskType = "estuary.dev/task-type"
	// RClockBegin is a uint32 in big-endian 8-char hexadecimal notation,
	// which is the beginning rotated clock range (inclusive) managed by this shard.
	RClockBegin = "estuary.dev/rclock-begin"
	// RClockBeginMin is the minimum possible RClock.
	RClockBeginMin = KeyBeginMin
	// RClockEnd is a uint32 in big-endian 8-char hexadecimal notation,
	// which is the ending rotated clock range (inclusive) managed by this shard.
	RClockEnd = "estuary.dev/rclock-end"
	// RClockEndMax is the maximum possible RClock.
	RClockEndMax = KeyEndMax
	// SplitTarget is the shard ID into which this shard is currently splitting.
	SplitTarget = "estuary.dev/split-target"
	// SplitSource is the shard ID from which this shard is currently splitting.
	SplitSource = "estuary.dev/split-source"
	// LogLevel is the desired log level for publishing logs related to the catalog task.
	LogLevel = "estuary.dev/log-level"
	// Journal to which task logs are directed.
	LogsJournal = "estuary.dev/logs-journal"
	// Journal to which task stats are directed.
	StatsJournal = "estuary.dev/stats-journal"

	Hostname = "estuary.dev/hostname"

	// ExposePort is the label key that is used to identify ports to be exposed by the container.
	// The values must be non-zero unsigned 16 bit integers. There may be multiple values for
	// this label, and all of them will be exposed. Each exposed port number may optionally
	// also specify a protocol or public visibility by specifying the respective label prefix,
	// suffixed by the port number. For example, `estuary.dev/port-proto/1883=mqtt` or
	// `estuary.dev/port-public/1883=true`.
	ExposePort       = "estuary.dev/expose-port"
	PortProtoPrefix  = "estuary.dev/port-proto/"
	PortPublicPrefix = "estuary.dev/port-public/"
)

// A re-exported subset of Gazette labels, defined in go.gazette.dev/core/labels/labels.go.
const (
	ContentType             = gazLabels.ContentType
	ContentType_JSONLines   = gazLabels.ContentType_JSONLines
	ContentType_RecoveryLog = gazLabels.ContentType_RecoveryLog
	ManagedBy               = gazLabels.ManagedBy
)

// IsRuntimeLabel returns whether the given |label| is managed by the Flow runtime,
// as opposed to the Flow control plane.
// Runtime labels and values use the data-plane's Etcd as their source of truth.
// Non-runtime labels are populated during the catalog build process,
// and use the catalog's models are their source-of-truth.
func IsRuntimeLabel(label string) bool {
	// If |label| has FieldPrefix as a prefix, its suffix is an encoded logical partition.
	if strings.HasPrefix(label, FieldPrefix) {
		return true
	}

	switch label {
	case
		// Cordoning is tracked in the data-plane.
		Cordon,
		// Key splits are performed dynamically by the runtime.
		KeyBegin, KeyEnd,
		// R-Clock splits are performed dynamically by the runtime.
		RClockBegin, RClockEnd,
		// Shard splits are performed dynamically by the runtime.
		SplitTarget, SplitSource:
		return true
	default:
		return false
	}
}
