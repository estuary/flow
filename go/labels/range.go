package labels

import (
	"fmt"
	"strconv"

	pf "github.com/estuary/protocols/flow"
)

// EncodeRange encodes the RangeSpec into the given LabelSet,
// which is then returned.
func EncodeRange(range_ pf.RangeSpec, set pf.LabelSet) pf.LabelSet {
	set = EncodeHexU32Label(KeyBegin, range_.KeyBegin, set)
	set = EncodeHexU32Label(KeyEnd, range_.KeyEnd, set)
	set = EncodeHexU32Label(RClockBegin, range_.RClockBegin, set)
	set = EncodeHexU32Label(RClockEnd, range_.RClockEnd, set)
	return set
}

// ParseRangeSpec extracts a RangeSpec from its associated labels.
func ParseRangeSpec(set pf.LabelSet) (pf.RangeSpec, error) {
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
func MustParseRangeSpec(set pf.LabelSet) pf.RangeSpec {
	if s, err := ParseRangeSpec(set); err != nil {
		panic(err)
	} else {
		return s
	}
}

// EncodeHexU32Label encodes label |name| as a hex-encoded uint32
// |value| into the provided |LabelSet|, which is returned.
func EncodeHexU32Label(name string, value uint32, set pf.LabelSet) pf.LabelSet {
	set.SetValue(name, fmt.Sprintf("%08x", value))
	return set
}

// ParseHexU32Label parses label |name|, a hex-encoded uint32, from the LabelSet.
// It returns an error if the label value is malformed.
func ParseHexU32Label(name string, set pf.LabelSet) (uint32, error) {
	if l, err := ExpectOne(set, name); err != nil {
		return 0, err
	} else if len(l) != 8 {
		return 0, fmt.Errorf("expected %s to be a 4-byte, hex encoded integer; got %v", name, l)
	} else if b, err := strconv.ParseUint(l, 16, 32); err != nil {
		return 0, fmt.Errorf("decoding hex-encoded label %s: %w", name, err)
	} else {
		return uint32(b), nil
	}
}
