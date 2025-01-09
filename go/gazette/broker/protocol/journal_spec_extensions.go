package protocol

import (
	"fmt"
	"io"
	"math"
	"mime"
	"strings"
	"text/template"
	"time"

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

// MaxReplication is a global bound on the degree of replication desired
// by any JournalSpec.
var MaxReplication int32 = math.MaxInt32

// SplitMeta splits off a ";meta" path segment of this Journal, separately
// returning the Journal and the ";meta" remainder (including leading
// ';' delimiter). If there is no metadata segment, the Journal is returned
// with an empty string.
func (n Journal) SplitMeta() (Journal, string) {
	var ind = strings.IndexByte(n.String(), ';')
	if ind != -1 {
		return n[:ind], n[ind:].String()
	}
	return n, ""
}

// StripMeta returns this Journal with a ";meta" suffix removed.
func (n Journal) StripMeta() Journal {
	var nn, _ = n.SplitMeta()
	return nn
}

// Validate returns an error if the Journal is not well-formed. It must draw
// from a restricted set of allowed path runes, be a clean path (as defined by
// path.Clean), and must not begin with a '/'. A Journal metadata component, if
// present, must similarly consist only of allowed token runes.
func (n Journal) Validate() error {
	var name, meta = n.SplitMeta()

	if err := ValidatePathComponent(name.String(), minJournalNameLen, maxJournalNameLen); err != nil {
		return err
	} else if meta == "" {
		// No metadata segment to validate.
	} else if err = ValidatePathComponent(meta[1:], 1, maxJournalNameLen); err != nil {
		return ExtendContext(err, "metadata path segment")
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
	} else if _, meta := m.Name.SplitMeta(); meta != "" {
		return NewValidationError("Name cannot have a metadata path segment (%s; expected no segment)", meta)
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
	} else if m.MaxAppendRate < 0 {
		return NewValidationError("invalid MaxAppendRate (%d; expected >= 0)", m.MaxAppendRate)
	} else if err = m.Suspend.Validate(); err != nil {
		return ExtendContext(err, "Suspend")
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

	// Ensure the PathPostfixTemplate parses and evaluates without
	// error over a zero-valued struct having the proper shape.
	if tpl, err := template.New("postfix").Parse(m.PathPostfixTemplate); err != nil {
		return ExtendContext(NewValidationError(err.Error()), "PathPostfixTemplate")
	} else if err = tpl.Execute(io.Discard, struct {
		Spool struct {
			Fragment
			FirstAppendTime time.Time
			Registers       LabelSet
		}
		JournalSpec
	}{}); err != nil {
		return ExtendContext(NewValidationError(err.Error()), "PathPostfixTemplate")
	}

	// Retention requires no explicit validation (all values permitted).

	return nil
}

func (x JournalSpec_Suspend_Level) Validate() error {
	switch x {
	case JournalSpec_Suspend_NONE, JournalSpec_Suspend_PARTIAL, JournalSpec_Suspend_FULL:
		return nil
	default:
		return NewValidationError("invalid Level variant (%s)", x)
	}
}

func (x JournalSpec_Suspend_Level) MarshalYAML() (interface{}, error) {
	if s, ok := JournalSpec_Suspend_Level_name[int32(x)]; ok {
		return s, nil
	} else {
		return int(x), nil
	}
}

func (x *JournalSpec_Suspend_Level) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Directly map YAML integer to flag.
	var i int
	if err := unmarshal(&i); err == nil {
		*x = JournalSpec_Suspend_Level(i)
		return nil
	}
	// Otherwise, expect a YAML string which matches an enum name.
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if tag, ok := JournalSpec_Suspend_Level_value[str]; !ok {
		return fmt.Errorf("%q is not a valid JournalSpec_Suspend_Level (options are %v)", str, JournalSpec_Suspend_Level_value)
	} else {
		*x = JournalSpec_Suspend_Level(tag)
		return nil
	}
}

func (m *JournalSpec_Suspend) GetLevel() JournalSpec_Suspend_Level {
	if m == nil {
		return JournalSpec_Suspend_NONE
	} else {
		return m.Level
	}
}

func (m *JournalSpec_Suspend) GetOffset() int64 {
	if m == nil {
		return 0
	} else {
		return m.Offset
	}
}

func (m *JournalSpec_Suspend) Validate() error {
	if err := m.GetLevel().Validate(); err != nil {
		return ExtendContext(err, "Level")
	} else if m.GetOffset() < 0 {
		return ExtendContext(NewValidationError("invalid Offset (%d; expected >= 0)", m.GetOffset()), "Offset")
	}
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
func (m *JournalSpec) DesiredReplication() int {
	var r = m.Replication

	if m.Suspend.GetLevel() == JournalSpec_Suspend_PARTIAL {
		r = 1 // Journal is suspended down to a single read-only replica.
	} else if m.Suspend.GetLevel() == JournalSpec_Suspend_FULL {
		r = 0 // Journal is suspended down to zero replicas.
	}

	if r > MaxReplication {
		r = MaxReplication
	}
	return int(r)
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
	if a.Fragment.PathPostfixTemplate == "" {
		a.Fragment.PathPostfixTemplate = b.Fragment.PathPostfixTemplate
	}
	if a.Flags == JournalSpec_NOT_SPECIFIED {
		a.Flags = b.Flags
	}
	if a.MaxAppendRate == 0 {
		a.MaxAppendRate = b.MaxAppendRate
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
	if a.Fragment.PathPostfixTemplate != b.Fragment.PathPostfixTemplate {
		a.Fragment.PathPostfixTemplate = ""
	}
	if a.Flags != b.Flags {
		a.Flags = JournalSpec_NOT_SPECIFIED
	}
	if a.MaxAppendRate != b.MaxAppendRate {
		a.MaxAppendRate = 0
	}
	a.Suspend = nil

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
	if a.Fragment.PathPostfixTemplate == b.Fragment.PathPostfixTemplate {
		a.Fragment.PathPostfixTemplate = ""
	}
	if a.Flags == b.Flags {
		a.Flags = JournalSpec_NOT_SPECIFIED
	}
	if a.MaxAppendRate == b.MaxAppendRate {
		a.MaxAppendRate = 0
	}
	return a
}

// LabelSetExt adds additional metadata labels to the LabelSet of the JournalSpec,
// returning the result. The result is built by truncating `buf` and then appending
// the merged LabelSet.
func (m *JournalSpec) LabelSetExt(buf LabelSet) LabelSet {
	return UnionLabelSets(LabelSet{
		Labels: []Label{
			{Name: "name", Value: m.Name.String()},
		},
	}, m.LabelSet, buf)
}

// validateJournalLabelConstraints asserts expected invariants of MessageType,
// MessageSubType, and ContentType labels:
//   - ContentType must parse as a RFC 1521 MIME / media-type.
//   - If MessageType is present, so is ContentType.
//   - If MessageSubType is present, so is MessageType.
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
