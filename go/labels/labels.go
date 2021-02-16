package labels

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

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
	// KeyEnd is a hexadecimal encoding of the ending key range (exclusive)
	// managed by this journal or shard, in an order-preserving packed []byte embedding.
	KeyEnd = "estuary.dev/key-end"
	// ManagedBy_Flow is a value for the Gazette labels.ManagedBy label.
	ManagedBy_Flow = "estuary.dev/flow"
)

// ShardSpec labels.
const (
	// CatalogURL is the URL of the catalog that's processed by this Shard.
	// The CatalogURL of a ShardSpec may change over time.
	// A running consumer detects and applies changes to the CatalogURL.
	CatalogURL = "estuary.dev/catalog-url"
	// Derivation is the name of the Estuary collection to be derived.
	// Once set on a ShardSpec, it cannot change.
	Derivation = "estuary.dev/derivation"
	// MaterializationTarget is the name of the materialization target, which is used to lookup
	// connection information in the catalog database. Once set on a ShardSpec, it cannot change.
	MaterializationTarget = "estuary.dev/materialization-target"
	// MaterializationTableName identifies the name of the table with the remote system to
	// materialize into.
	MaterializationTableName = "estuary.dev/materialization-table"
	// RClockBegin is a uint64 in big-endian 16-char hexadecimal notation,
	// which is the beginning rotated clock range (inclusive) managed by this shard.
	RClockBegin = "estuary.dev/rclock-begin"
	// RClockEnd is a uint64 in big-endian 16-char hexadecimal notation,
	// which is the ending rotated clock range (exclusive) managed by this shard.
	RClockEnd = "estuary.dev/rclock-end"
)

// ParseRangeSpec extracts a RangeSpec from its associated labels.
func ParseRangeSpec(set pb.LabelSet) (pf.RangeSpec, error) {
	if kb, err := mustHexLabel(KeyBegin, set, -1); err != nil {
		return pf.RangeSpec{}, err
	} else if ke, err := mustHexLabel(KeyEnd, set, -1); err != nil {
		return pf.RangeSpec{}, err
	} else if cb, err := mustHexLabel(RClockBegin, set, 8); err != nil {
		return pf.RangeSpec{}, err
	} else if ce, err := mustHexLabel(RClockEnd, set, 8); err != nil {
		return pf.RangeSpec{}, err
	} else {
		var out = pf.RangeSpec{
			KeyBegin:    kb,
			KeyEnd:      ke,
			RClockBegin: binary.BigEndian.Uint64(cb),
			RClockEnd:   binary.BigEndian.Uint64(ce),
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

func mustHexLabel(name string, set pb.LabelSet, length int) ([]byte, error) {
	if l := set.ValuesOf(name); len(l) != 1 {
		return nil, fmt.Errorf("missing required label: %s", name)
	} else if b, err := hex.DecodeString(l[0]); err != nil {
		return nil, fmt.Errorf("decoding hex label %s, value %v: %w", name, l[0], err)
	} else if length != -1 && len(b) != length {
		return nil, fmt.Errorf("label %s value %x has unexpected length (%d; expected %d)", name, b, len(b), length)
	} else {
		return b, nil
	}
}
