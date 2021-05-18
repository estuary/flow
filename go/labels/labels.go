package labels

import (
	"fmt"
	"strconv"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// JournalSpec & ShardSpec labels.
const (
	// Collection is the name of the Estuary collection for which this Journal
	// holds documents.
	Collection = "estuary.dev/collection"
	// Field is a logical partition of the Collection that's implemented by this
	// journal.
	FieldPrefix = "estuary.dev/field/"
	// KeyBegin is a hexadecimal encoding of the beginning key range (inclusive)
	// managed by this journal or shard, in an order-preserving packed []byte embedding.
	KeyBegin = "estuary.dev/key-begin"
	// KeyBeginMin is the minimum possible key.
	KeyBeginMin = "00000000"
	// KeyEnd is a hexadecimal encoding of the ending key range (exclusive)
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
	// TaskTypeDerivation is a "derivation" TaskType.
	TaskTypeDerivation = "derivation"
	// TaskTypeMaterialization is a "materialization" TaskType.
	TaskTypeMaterialization = "materialization"
	// RClockBegin is a uint64 in big-endian 16-char hexadecimal notation,
	// which is the beginning rotated clock range (inclusive) managed by this shard.
	RClockBegin = "estuary.dev/rclock-begin"
	// RClockBeginMin is the minimum possible RClock.
	RClockBeginMin = KeyBeginMin
	// RClockEnd is a uint64 in big-endian 16-char hexadecimal notation,
	// which is the ending rotated clock range (exclusive) managed by this shard.
	RClockEnd = "estuary.dev/rclock-end"
	// RClockEndMax is the maximum possible RClock.
	RClockEndMax = KeyEndMax

	// SplitTarget is the shard ID into which this shard is currently splitting.
	SplitTarget = "estuary.dev/split-target"
	// SplitSource is the shard ID from which this shard is currently splitting.
	SplitSource = "estuary.dev/split-source"
)

// EncodeRange encodes the RangeSpec into the given LabelSet,
// which is then returned.
func EncodeRange(range_ pf.RangeSpec, set pb.LabelSet) pb.LabelSet {
	EncodeHexU32Label(KeyBegin, range_.KeyBegin, &set)
	EncodeHexU32Label(KeyEnd, range_.KeyEnd, &set)
	EncodeHexU32Label(RClockBegin, range_.RClockBegin, &set)
	EncodeHexU32Label(RClockEnd, range_.RClockEnd, &set)
	return set
}

// ParseRangeSpec extracts a RangeSpec from its associated labels.
func ParseRangeSpec(set pb.LabelSet) (pf.RangeSpec, error) {
	if kb, err := ParseHexU32Label(KeyBegin, set); err != nil {
		return pf.RangeSpec{}, err
	} else if ke, err := ParseHexU32Label(KeyEnd, set); err != nil {
		return pf.RangeSpec{}, err
	} else if cb, err := ParseHexU32Label(RClockBegin, set); err != nil {
		return pf.RangeSpec{}, err
	} else if ce, err := ParseHexU32Label(RClockEnd, set); err != nil {
		return pf.RangeSpec{}, err
	} else {
		var out = pf.RangeSpec{
			KeyBegin:    kb,
			KeyEnd:      ke,
			RClockBegin: cb,
			RClockEnd:   ce,
		}
		return out, out.Validate()
	}
}

// MustParseRangeSpec parses a RangeSpec from the labels, and panics on an error.
func MustParseRangeSpec(set pb.LabelSet) pf.RangeSpec {
	if s, err := ParseRangeSpec(set); err != nil {
		panic(err)
	} else {
		return s
	}
}

// EncodeHexU32Label encodes label |name| as a hex-encoded uint32
// |value| into the provided |LabelSet|, which is returned.
func EncodeHexU32Label(name string, value uint32, set *pb.LabelSet) {
	set.SetValue(name, fmt.Sprintf("%08x", value))
}

// ParseHexU32Label parses label |name|, a hex-encoded uint32, from the LabelSet.
// It returns an error if the label value is malformed.
func ParseHexU32Label(name string, set pb.LabelSet) (uint32, error) {
	if l := set.ValuesOf(name); len(l) != 1 {
		return 0, fmt.Errorf("missing required label: %s", name)
	} else if len(l[0]) != 8 {
		return 0, fmt.Errorf("expected %s to be a 4-byte, hex encoded integer; got %v", name, l[0])
	} else if b, err := strconv.ParseUint(l[0], 16, 32); err != nil {
		return 0, fmt.Errorf("decoding hex-encoded label %s: %w", name, err)
	} else {
		return uint32(b), nil
	}
}

// BuildShardID builds the ShardID that's implied by the LabelSet.
func BuildShardID(set pb.LabelSet) (pc.ShardID, error) {
	// Pluck singleton label values we expect to exist.
	var (
		type_, err1       = valueOf(set, TaskType)
		name, err2        = valueOf(set, TaskName)
		keyBegin, err3    = valueOf(set, KeyBegin)
		rclockBegin, err4 = valueOf(set, RClockBegin)
	)
	for _, err := range []error{err1, err2, err3, err4} {
		if err != nil {
			return "", err
		}
	}

	switch type_ {
	case TaskTypeDerivation:
		type_ = "derivation"
	case TaskTypeMaterialization:
		type_ = "materialize"
	default:
		return "", fmt.Errorf("unexpected %s: %s", TaskType, type_)
	}

	return pc.ShardID(
		fmt.Sprintf("%s/%s/%s-%s", type_, name, keyBegin, rclockBegin),
	), nil
}

func valueOf(set pb.LabelSet, name string) (string, error) {
	var v = set.ValuesOf(name)
	if len(v) != 1 {
		return "", fmt.Errorf("expected one %s: %v", name, v)
	} else {
		return v[0], nil
	}
}
