package labels

import (
	"fmt"
	"strconv"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
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
	// TaskTypeCapture is a "capture" TaskType
	TaskTypeCapture = "capture"
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
)

// EncodeRange encodes the RangeSpec into the given LabelSet,
// which is then returned.
func EncodeRange(range_ pf.RangeSpec, set pb.LabelSet) pb.LabelSet {
	set.AddValue(KeyBegin, fmt.Sprintf("%08x", range_.KeyBegin))
	set.AddValue(KeyEnd, fmt.Sprintf("%08x", range_.KeyEnd))
	set.AddValue(RClockBegin, fmt.Sprintf("%08x", range_.RClockBegin))
	set.AddValue(RClockEnd, fmt.Sprintf("%08x", range_.RClockEnd))
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
