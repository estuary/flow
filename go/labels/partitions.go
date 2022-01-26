package labels

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// EncodePartitionValue appends an encoding of |value| into the
// []byte slice, returning the result. Encoded values are suitable
// for embedding within Journal names as well as label values.
func EncodePartitionValue(b []byte, value tuple.TupleElement) []byte {
	switch v := value.(type) {
	case nil:
		return append(b, "null"...)
	case bool:
		if v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case string:
		// Label values have a pretty restrictive set of allowed non-letter
		// or digit characters. Use URL query escapes to encode an arbitrary
		// string value into a label-safe (and name-safe) representation.
		return append(b, url.QueryEscape(v)...)
	default:
		panic(fmt.Sprintf("invalid element type: %#v", value))
	}
}

// EncodePartitionLabels adds encoded |fields| and corresponding |values|
// into the given LabelSet. |fields| must be in sorted order and have the
// same length as |values|, or EncodePartitionLabels panics.
func EncodePartitionLabels(fields []string, values tuple.Tuple, set pf.LabelSet) pf.LabelSet {
	if len(fields) != len(values) {
		panic("fields and values have different lengths")
	}
	for i := range fields {
		if i > 0 && fields[i] <= fields[i-1] {
			panic("fields are not in sorted order")
		}
		set.AddValue(
			FieldPrefix+fields[i], // |fields| are already restricted to label-safe values.
			string(EncodePartitionValue(nil, values[i])),
		)
	}
	return set
}

// PartitionSuffix returns the Journal name suffix that's implied by the LabelSet.
// This suffix is appended to the collection name to form a complete journal name.
func PartitionSuffix(set pf.LabelSet) (string, error) {
	var name strings.Builder

	// We're relying on the fact that labels are always in lexicographic order.
	for _, l := range set.Labels {
		if !strings.HasPrefix(l.Name, FieldPrefix) {
			continue
		}

		name.WriteString(l.Name[len(FieldPrefix):]) // Field without label prefix.
		name.WriteByte('=')
		name.WriteString(l.Value)
		name.WriteByte('/')
	}
	name.WriteString("pivot=")

	// Pluck singleton label value we expect to exist.
	var keyBegin, err = valueOf(set, KeyBegin)
	if err != nil {
		return "", err
	}

	// As a prettified special case, and for historical reasons, we represent the
	// KeyBeginMin value of "00000000" as just "00". This is safe because "00"
	// will naturally order before all other splits, as "00000000" would.
	// All other key splits are their normal EncodeHexU32Label encodings.
	if keyBegin == KeyBeginMin {
		name.WriteString("00")
	} else {
		name.WriteString(keyBegin)
	}

	return name.String(), nil
}

// ShardSuffix is the suffix of a ShardID that's implied by the LabelSet.
// This suffix is appended to the tasks's base name to form a complete ShardID.
func ShardSuffix(set pf.LabelSet) (string, error) {
	// Pluck singleton label values we expect to exist.
	var (
		keyBegin, err1    = valueOf(set, KeyBegin)
		rclockBegin, err2 = valueOf(set, RClockBegin)
	)
	for _, err := range []error{err1, err2} {
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%s-%s", keyBegin, rclockBegin), nil
}

func valueOf(set pf.LabelSet, name string) (string, error) {
	var v = set.ValuesOf(name)
	if len(v) != 1 {
		return "", fmt.Errorf("expected one %s: %v", name, v)
	} else {
		return v[0], nil
	}
}
