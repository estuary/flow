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
//
// * String values append their URL query-encoding.
// * Booleans append either %_true or %_false
// * Integers append their base-10 encoding with a `%_` prefix, as in `%_-1234`.
// * Null appends %_null.
//
// Note that types *other* than strings all use a common %_ prefix, which can
// never be produced by a query-encoded string and thus allows for unambiguously
// mapping a partition value back into its JSON value.
func EncodePartitionValue(b []byte, value tuple.TupleElement) []byte {
	switch v := value.(type) {
	case nil:
		return append(b, `%_null`...)
	case bool:
		if v {
			return append(b, `%_true`...)
		}
		return append(b, `%_false`...)
	case uint64:
		return strconv.AppendUint(append(b, `%_`...), v, 10)
	case int64:
		return strconv.AppendInt(append(b, `%_`...), v, 10)
	case int:
		return strconv.AppendInt(append(b, `%_`...), int64(v), 10)
	case string:
		// Label values have a pretty restrictive set of allowed non-letter
		// or digit characters. Use URL query escapes to encode an arbitrary
		// string value into a label-safe (and name-safe) representation.
		return append(b, strings.ReplaceAll(url.QueryEscape(v), "+", "%20")...)
	default:
		panic(fmt.Sprintf("invalid element type: %#v", value))
	}
}

// DecodePartitionValue maps a partition value encoding produced by
// EncodePartitionValue back into its dynamic TupleElement type.
func DecodePartitionValue(value string) (tuple.TupleElement, error) {
	if value == "%_null" {
		return nil, nil
	} else if value == "%_true" {
		return true, nil
	} else if value == "%_false" {
		return false, nil
	} else if strings.HasPrefix(value, "%_-") {
		return strconv.ParseInt(value[2:], 10, 64)
	} else if strings.HasPrefix(value, "%_") {
		return strconv.ParseUint(value[2:], 10, 64)
	} else {
		return url.QueryUnescape(value)
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

// DecodePartitionLabels decodes |fields| from a |set| of labels,
// returning a Tuple having the same size and order as |fields|.
func DecodePartitionLabels(fields []string, set pf.LabelSet) (tuple.Tuple, error) {
	var out tuple.Tuple
	for _, field := range fields {
		if value, err := valueOf(set, FieldPrefix+field); err != nil {
			return nil, err
		} else if elem, err := DecodePartitionValue(value); err != nil {
			return nil, fmt.Errorf("decoding field %s value %q: %w", field, value, err)
		} else {
			out = append(out, elem)
		}
	}
	return out, nil
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
