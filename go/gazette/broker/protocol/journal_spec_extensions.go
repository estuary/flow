package protocol

import (
	"fmt"
	"mime"
	"path"
	"strings"
	"time"

	"go.gazette.dev/core/allocator"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/labels"
)

// Journal uniquely identifies a journal brokered by Gazette.
// By convention, journals are named using a forward-slash notation which
// captures their hierarchical relationships into organizations, topics and
// partitions. For example, a Journal might be:
// "company-journals/interesting-topic/part-1234"
type Journal string

// Offset is a byte offset of a Journal.
type Offset = int64

// Offsets is a map of Journals and Offsets.
type Offsets map[Journal]Offset

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

// Validate returns an error if the Offsets are not well-formed.
func (o Offsets) Validate() error {
	for j, o := range o {
		var err = j.Validate()
		if err == nil && o < 0 {
			err = NewValidationError("invalid offset (%d; expected >= 0)", o)
		}
		if err != nil {
			return ExtendContext(err, "Offsets[%s]", j)
		}
	}
	return nil
}

// Copy allocates and returns a copy of Offsets.
func (o Offsets) Copy() Offsets {
	var out = make(Offsets, len(o))
	for j, o := range o {
		out[j] = o
	}
	return out
}

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
	} else if err = validateJournalLabelConstraints(m.LabelSet); err != nil {
		return ExtendContext(err, "Labels")
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

	if m.FlushInterval != 0 && m.FlushInterval < minFlushInterval {
		return NewValidationError("invalid FlushInterval (%s; expected >= %s)",
			m.FlushInterval, minFlushInterval)
	}

	// Retention requires no explicit validation (all values permitted).

	return nil
}

// Validate returns an error if the JournalSpec_Flag is malformed.
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

// MarshalYAML maps the JournalSpec_Flag to a YAML value.
func (x JournalSpec_Flag) MarshalYAML() (interface{}, error) {
	if s, ok := JournalSpec_Flag_name[int32(x)]; ok {
		return s, nil
	} else {
		return int(x), nil
	}
}

// UnmarshalYAML maps a YAML integer directly to a Flag value, or a YAML string
// to a Flag with corresponding enum name.
func (x *JournalSpec_Flag) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Directly map YAML integer to flag.
	var i int
	if err := unmarshal(&i); err == nil {
		*x = JournalSpec_Flag(i)
		return nil
	}
	// Otherwise, expect a YAML string which matches an enum name.
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if tag, ok := JournalSpec_Flag_value[str]; !ok {
		return fmt.Errorf("%q is not a valid JournalSpec_Flag (options are %v)", str, JournalSpec_Flag_value)
	} else {
		*x = JournalSpec_Flag(tag)
		return nil
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

// DesiredReplication returns the configured Replication of the spec. It
// implements allocator.ItemValue.
func (m *JournalSpec) DesiredReplication() int { return int(m.Replication) }

// IsConsistent returns true if the Route stored under each of |assignments|
// agrees with the Route implied by the |assignments| keys. It implements
// allocator.ItemValue.
func (m *JournalSpec) IsConsistent(_ keyspace.KeyValue, assignments keyspace.KeyValues) bool {
	var rt Route
	rt.Init(assignments)

	return JournalRouteMatchesAssignments(rt, assignments)
}

// JournalRouteMatchesAssignments returns true iff the Route is equivalent to the
// Route marshaled with each of the journal's |assignments|.
func JournalRouteMatchesAssignments(rt Route, assignments keyspace.KeyValues) bool {
	for _, a := range assignments {
		if !rt.Equivalent(a.Decoded.(allocator.Assignment).AssignmentValue.(*Route)) {
			return false
		}
	}
	return len(rt.Members) == len(assignments)
}

// UnionJournalSpecs returns a JournalSpec combining all non-zero-valued fields
// across |a| and |b|. Where both |a| and |b| provide a non-zero value for
// a field, the value of |a| is retained.
func UnionJournalSpecs(a, b JournalSpec) JournalSpec {
	if a.Replication == 0 {
		a.Replication = b.Replication
	}
	a.LabelSet = UnionLabelSets(a.LabelSet, b.LabelSet, LabelSet{})

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
	if a.Fragment.FlushInterval == 0 {
		a.Fragment.FlushInterval = b.Fragment.FlushInterval
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
	if a.Fragment.FlushInterval != b.Fragment.FlushInterval {
		a.Fragment.FlushInterval = 0
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
	if a.Fragment.FlushInterval == b.Fragment.FlushInterval {
		a.Fragment.FlushInterval = 0
	}
	if a.Flags == b.Flags {
		a.Flags = JournalSpec_NOT_SPECIFIED
	}
	return a
}

// ExtractJournalSpecMetaLabels adds to the LabelSet a singular label "name",
// with value of the JournalSpec Name, and multi-label "prefix", having a value
// for each path component prefix of Name.
func ExtractJournalSpecMetaLabels(spec *JournalSpec, out LabelSet) LabelSet {
	var name = spec.Name.String()
	out.Labels = append(out.Labels[:0], Label{Name: "name", Value: name})

	for i, j := 0, strings.IndexByte(name, '/'); j != -1; j = strings.IndexByte(name[i:], '/') {
		i += j + 1
		out.Labels = append(out.Labels, Label{Name: "prefix", Value: name[:i]})
	}
	return out
}

// validateJournalLabelConstraints asserts expected invariants of MessageType,
// MessageSubType, and ContentType labels:
//  * ContentType must parse as a RFC 1521 MIME / media-type.
//  * If MessageType is present, ContentType must be present and match a known framing.
//  * If MessageSubType is present, so is MessageType.
func validateJournalLabelConstraints(ls LabelSet) error {
	if err := ValidateSingleValueLabels(ls); err != nil {
		return err
	}
	var ct = ls.ValuesOf(labels.ContentType)
	if ct != nil {
		if _, _, err := mime.ParseMediaType(ct[0]); err != nil {
			return NewValidationError("parsing %s: %s", labels.ContentType, err)
		}
	}
	if mt := ls.ValuesOf(labels.MessageType); mt != nil {
		if ct == nil {
			return NewValidationError("expected %s label alongside %s", labels.ContentType, labels.MessageType)
		} else if _, ok := labels.FramedContentTypes[ct[0]]; !ok {
			return NewValidationError("%s label is not a known message framing (%s; expected one of %v)",
				labels.ContentType, ct[0], labels.FramedContentTypes)
		}
	} else if mst := ls.ValuesOf(labels.MessageSubType); mst != nil {
		return NewValidationError("expected %s label alongside %s", labels.MessageType, labels.MessageSubType)
	}
	return nil
}

const (
	minJournalNameLen, maxJournalNameLen   = 4, 512
	maxJournalReplication                  = 5
	minRefreshInterval, maxRefreshInterval = time.Second, time.Hour * 24
	minFlushInterval                       = time.Minute
	minFragmentLen, maxFragmentLen         = 1 << 10, 1 << 34 // 1024 => 17,179,869,184
)
