package protocol

import (
	"sort"
)

// Validate returns an error if the Label is not well-formed.
func (m Label) Validate() error {
	if err := ValidateB64Str(m.Name, minLabelLen, maxLabelLen); err != nil {
		return ExtendContext(err, "Name")
	} else if err = ValidateB64Str(m.Value, 0, maxLabelValueLen); err != nil {
		return ExtendContext(err, "Value")
	}
	return nil
}

// Validate returns an error if the LabelSet is not well-formed.
func (m LabelSet) Validate() error {
	for i := range m.Labels {
		if err := m.Labels[i].Validate(); err != nil {
			return ExtendContext(err, "Labels[%d]", i)
		}
		if i != 0 && m.Labels[i].Name <= m.Labels[i-1].Name {
			return NewValidationError("Labels not in unique, sorted order (index %d; %+v <= %+v)",
				i, m.Labels[i].Name, m.Labels[i-1].Name)
		}
	}
	return nil
}

// ValueOf returns the value of Label |name|, if it exists in the LabelSet.
func (m LabelSet) ValueOf(name string) (value string, ok bool) {
	var ind = sort.Search(len(m.Labels),
		func(i int) bool { return m.Labels[i].Name >= name })

	if ind < len(m.Labels) && m.Labels[ind].Name == name {
		value, ok = m.Labels[ind].Value, true
	}
	return
}

// UnionLabelSets returns labels present in any of the provided LabelSets. Where
// multiple LabelSets provided the same label, the first value specified in
// LabelSet argument order is retained.
func UnionLabelSets(sets ...LabelSet) (out LabelSet) {
	for _, set := range sets {
		var io, is int

		for io != len(out.Labels) && is != len(set.Labels) {
			if lo, ls := out.Labels[io], set.Labels[is]; lo.Name == ls.Name {
				// Prefer an existing label value in |out| over its alternate in |set|.
				io, is = io+1, is+1
			} else if lo.Name < ls.Name {
				io += 1 // Label in |out| not in |set|.
			} else {
				// Insert label in |set| not in |out|.
				out.Labels = append(out.Labels, Label{})
				copy(out.Labels[io+1:], out.Labels[io:])
				out.Labels[io] = ls
				io, is = io+1, is+1
			}
		}
		for is != len(set.Labels) {
			// Insert extra label in |set|.
			out.Labels = append(out.Labels, set.Labels[is])
			is += 1
		}
	}
	return out
}

// IntersectLabelSets returns labels which are present with matched values
// across all provided LabelSets.
func IntersectLabelSets(sets ...LabelSet) (out LabelSet) {
	for i, set := range sets {

		if i == 0 {
			// Trivially initialize with sets[0].
			out = LabelSet{Labels: append([]Label{}, sets[0].Labels...)}
			continue
		}

		var io, is int

		for io != len(out.Labels) && is != len(set.Labels) {
			if lo, ls := out.Labels[io], set.Labels[is]; lo == ls {
				// Label Name & Value matches. Keep it in the intersection.
				io, is = io+1, is+1
			} else if lo.Name <= ls.Name {
				// Mismatch. Remove label from |out|.
				copy(out.Labels[io:], out.Labels[io+1:])
				out.Labels = out.Labels[:len(out.Labels)-1]
			} else {
				// Skip label in |set| not appearing in |out|.
				is += 1
			}
		}
		out.Labels = out.Labels[:io] // Trim any remainder not appearing in |set|.
	}

	if len(out.Labels) == 0 {
		out.Labels = nil
	}
	return
}

// SubtractLabelSets returns labels in |a| which are not also in |b|.
func SubtractLabelSet(a, b LabelSet) LabelSet {
	var out = LabelSet{Labels: append([]Label{}, a.Labels...)}

	var io, ib int
	for io != len(out.Labels) && ib != len(b.Labels) {
		if lo, lb := out.Labels[io], b.Labels[ib]; lo == lb {
			// Match. Remove label from |out| also appearing in |b|.
			copy(out.Labels[io:], out.Labels[io+1:])
			out.Labels = out.Labels[:len(out.Labels)-1]
		} else if lo.Name <= lb.Name {
			io += 1
		} else {
			ib += 1
		}
	}

	if len(out.Labels) == 0 {
		out.Labels = nil
	}
	return out
}

// Validate returns an error if the LabelSelector is not well-formed.
func (m LabelSelector) Validate() error {
	if err := m.Include.Validate(); err != nil {
		return ExtendContext(err, "Include")
	} else if err := m.Exclude.Validate(); err != nil {
		return ExtendContext(err, "Exclude")
	}
	return nil
}

// Matches returns whether the LabelSet is matched by the LabelSelector.
func (m LabelSelector) Matches(s LabelSet) bool {
	if matchLabels(m.Exclude.Labels, s.Labels) {
		return false
	} else if len(m.Include.Labels) != 0 && !matchLabels(m.Include.Labels, s.Labels) {
		return false
	}
	return true
}

func matchLabels(s, o []Label) bool {
	for len(s) != 0 && len(o) != 0 {
		if s[0].Name < o[0].Name {
			s = s[1:]
		} else if s[0].Name > o[0].Name {
			o = o[1:]
		} else if s[0].Value == o[0].Value || s[0].Value == "" {
			return true
		} else {
			s, o = s[1:], o[1:]
		}
	}
	return false
}

const (
	minLabelLen, maxLabelLen = 2, 64
	maxLabelValueLen         = 1024
)
