package protocol

import (
	"path"
	"strings"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
)

// Journal uniquely identifies a journal brokered by Gazette.
// By convention, journals are named using a forward-slash notation which
// captures their hierarchical relationships into organizations, topics and
// partitions. For example, a Journal might be:
// "company-journals/interesting-topic/part-1234"
type Journal string

// Validate returns an error if the Journal is not well-formed. It must be of
// the base64 alphabet, a clean path (as defined by path.Clean), and must not
// begin with a '/'.
func (n Journal) Validate() error {
	if err := ValidateToken(n.String(), minJournalNameLen, maxJournalNameLen); err != nil {
		return err
	} else if path.Clean(n.String()) != n.String() {
		return NewValidationError("must be a clean path (%s)", n)
	} else if n[0] == '/' {
		return NewValidationError("cannot begin with '/' (%s)", n)
	}
	return nil
}

// String returns the Journal as a string.
func (n Journal) String() string { return string(n) }

// Validate returns an error if the JournalSpec is not well-formed.
func (m *JournalSpec) Validate() error {
	if err := m.Name.Validate(); err != nil {
		return ExtendContext(err, "Name")
	} else if m.Replication < 1 || m.Replication > maxJournalReplication {
		return NewValidationError("invalid Replication (%d; expected 1 <= Replication <= %d)",
			m.Replication, maxJournalReplication)
	} else if err = m.LabelSet.Validate(); err != nil {
		return ExtendContext(err, "Labels")
	} else if len(m.LabelSet.ValuesOf("name")) != 0 {
		return NewValidationError(`Labels cannot include label "name"`)
	} else if len(m.LabelSet.ValuesOf("prefix")) != 0 {
		return NewValidationError(`Labels cannot include label "prefix"`)
	} else if err = validateFramingLabel(m.LabelSet); err != nil {
		return err
	} else if err = m.Fragment.Validate(); err != nil {
		return ExtendContext(err, "Fragment")
	} else if err = m.Flags.Validate(); err != nil {
		return ExtendContext(err, "Flags")
	}

	return nil
}

// Validate returns an error if the JournalSpec_Fragment is not well-formed.
func (m *JournalSpec_Fragment) Validate() error {
	if m.Length < minFragmentLen || m.Length > maxFragmentLen {
		return NewValidationError("invalid Length (%d; expected %d <= length <= %d)",
			m.Length, minFragmentLen, maxFragmentLen)
	} else if err := m.CompressionCodec.Validate(); err != nil {
		return ExtendContext(err, "CompressionCodec")
	}
	for i, store := range m.Stores {
		if err := store.Validate(); err != nil {
			return ExtendContext(err, "Stores[%d]", i)
		}
		if i == 0 && store.URL().Scheme == "file" &&
			m.CompressionCodec == CompressionCodec_GZIP_OFFLOAD_DECOMPRESSION {
			return NewValidationError("GZIP_OFFLOAD_DECOMPRESSION is incompatible with file:// stores (%s)", store)
		}
	}
	if m.RefreshInterval < minRefreshInterval || m.RefreshInterval > maxRefreshInterval {
		return NewValidationError("invalid RefreshInterval (%s; expected %s <= interval <= %s)",
			m.RefreshInterval, minRefreshInterval, maxRefreshInterval)
	}

	// Retention requires no explicit validation (all values permitted).

	return nil
}

func (x JournalSpec_Flag) Validate() error {
	switch x {
	case JournalSpec_NOT_SPECIFIED, JournalSpec_O_WRONLY, JournalSpec_O_RDONLY, JournalSpec_O_RDWR:
		return nil
	default:
		return NewValidationError("invalid combination (%s)", x)
	}
}

// MayRead returns whether reads are permitted.
func (x JournalSpec_Flag) MayRead() bool {
	switch x {
	case JournalSpec_NOT_SPECIFIED, JournalSpec_O_RDONLY, JournalSpec_O_RDWR:
		return true
	default:
		return false
	}
}

// MayWrite returns whether writes are permitted.
func (x JournalSpec_Flag) MayWrite() bool {
	switch x {
	case JournalSpec_NOT_SPECIFIED, JournalSpec_O_WRONLY, JournalSpec_O_RDWR:
		return true
	default:
		return false
	}
}

// MarshalString returns the marshaled encoding of the JournalSpec as a string.
func (m *JournalSpec) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// v3_allocator.ItemValue implementation.
func (m *JournalSpec) DesiredReplication() int { return int(m.Replication) }

// IsConsistent returns true if the Route stored under each of |assignments|
// agrees with the Route implied by the |assignments| keys.
func (m *JournalSpec) IsConsistent(_ keyspace.KeyValue, assignments keyspace.KeyValues) bool {
	if !m.Flags.MayWrite() {
		return true // Read-only journals can never become inconsistent.
	}

	var rt Route
	rt.Init(assignments)

	for _, a := range assignments {
		if !rt.Equivalent(a.Decoded.(allocator.Assignment).AssignmentValue.(*Route)) {
			return false
		}
	}
	return true
}

// UnionJournalSpecs returns a JournalSpec combining all non-zero-valued fields
// across |a| and |b|. Where both |a| and |b| provide a non-zero value for
// a field, the value of |a| is retained.
func UnionJournalSpecs(a, b JournalSpec) JournalSpec {
	if a.Replication == 0 {
		a.Replication = b.Replication
	}
	a.LabelSet = UnionLabelSets(b.LabelSet, a.LabelSet, LabelSet{})

	if a.Fragment.Length == 0 {
		a.Fragment.Length = b.Fragment.Length
	}
	if a.Fragment.CompressionCodec == CompressionCodec_INVALID {
		a.Fragment.CompressionCodec = b.Fragment.CompressionCodec
	}
	if a.Fragment.Stores == nil {
		a.Fragment.Stores = b.Fragment.Stores
	}
	if a.Fragment.RefreshInterval == 0 {
		a.Fragment.RefreshInterval = b.Fragment.RefreshInterval
	}
	if a.Fragment.Retention == 0 {
		a.Fragment.Retention = b.Fragment.Retention
	}
	if a.Flags == JournalSpec_NOT_SPECIFIED {
		a.Flags = b.Flags
	}
	return a
}

// IntersectJournalSpecs returns a JournalSpec having a non-zero-valued field
// for each field value which is shared between |a| and |b|.
func IntersectJournalSpecs(a, b JournalSpec) JournalSpec {
	if a.Replication != b.Replication {
		a.Replication = 0
	}
	a.LabelSet = IntersectLabelSets(a.LabelSet, b.LabelSet, LabelSet{})

	if a.Fragment.Length != b.Fragment.Length {
		a.Fragment.Length = 0
	}
	if a.Fragment.CompressionCodec != b.Fragment.CompressionCodec {
		a.Fragment.CompressionCodec = CompressionCodec_INVALID
	}
	if !fragmentStoresEq(a.Fragment.Stores, b.Fragment.Stores) {
		a.Fragment.Stores = nil
	}
	if a.Fragment.RefreshInterval != b.Fragment.RefreshInterval {
		a.Fragment.RefreshInterval = 0
	}
	if a.Fragment.Retention != b.Fragment.Retention {
		a.Fragment.Retention = 0
	}
	if a.Flags != b.Flags {
		a.Flags = JournalSpec_NOT_SPECIFIED
	}
	return a
}

// SubtractJournalSpecs returns a JournalSpec derived from |a| but having a
// zero-valued field for each field which is matched by |b|.
func SubtractJournalSpecs(a, b JournalSpec) JournalSpec {
	if a.Replication == b.Replication {
		a.Replication = 0
	}
	a.LabelSet = SubtractLabelSet(a.LabelSet, b.LabelSet, LabelSet{})

	if a.Fragment.Length == b.Fragment.Length {
		a.Fragment.Length = 0
	}
	if a.Fragment.CompressionCodec == b.Fragment.CompressionCodec {
		a.Fragment.CompressionCodec = CompressionCodec_INVALID
	}
	if fragmentStoresEq(a.Fragment.Stores, b.Fragment.Stores) {
		a.Fragment.Stores = nil
	}
	if a.Fragment.RefreshInterval == b.Fragment.RefreshInterval {
		a.Fragment.RefreshInterval = 0
	}
	if a.Fragment.Retention == b.Fragment.Retention {
		a.Fragment.Retention = 0
	}
	if a.Flags == b.Flags {
		a.Flags = JournalSpec_NOT_SPECIFIED
	}
	return a
}

func ExtractJournalSpecMetaLabels(spec *JournalSpec, out LabelSet) LabelSet {
	var name = spec.Name.String()
	out.Labels = append(out.Labels[:0], Label{Name: "name", Value: name})

	for i, j := 0, strings.IndexByte(name, '/'); j != -1; j = strings.IndexByte(name[i:], '/') {
		i += j + 1
		out.Labels = append(out.Labels, Label{Name: "prefix", Value: name[:i]})
	}
	return out
}

// validateFramingLabel asserts that a "framing" label, if present, matches a
// restricted set of values permitted by the `message` package. Formally,
// this package has no (and should have no) dependency on `message`. However,
// in the interests of failing fast & providing useful feedback to users, we
// fold these checks into JournalSpec validation.
func validateFramingLabel(labels LabelSet) error {
	var f = labels.ValuesOf("framing")

	switch len(f) {
	case 0:
		return nil
	default:
		return NewValidationError(`Label "framing" cannot have multiple values`)
	case 1: // Pass.
	}
	switch f[0] {
	case FramingFixed, FramingJSON:
		return nil
	default:
		return NewValidationError(`Label "framing" contains an invalid value (%s)`, f[0])
	}
}

const (
	minJournalNameLen, maxJournalNameLen   = 4, 512
	maxJournalReplication                  = 5
	minRefreshInterval, maxRefreshInterval = time.Second, time.Hour * 24
	minFragmentLen, maxFragmentLen         = 1 << 10, 1 << 34 // 1024 => 17,179,869,184

	// FramingFixed is the label value for message.FixedFraming.
	FramingFixed = "fixed"
	// FramingJSON is the label value for message.JSONFraming.
	FramingJSON = "json"
)
