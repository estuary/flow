package testing

import (
	pb "go.gazette.dev/core/broker/protocol"
)

// MinClock reduces by taking the smallest offset of each common journal.
func MinClock(lhs, rhs pb.Offsets) pb.Offsets {
	lhs = lhs.Copy()

	for journal, r := range rhs {
		if l, ok := lhs[journal]; !ok {
			lhs[journal] = r
		} else if l > r {
			lhs[journal] = r
		}
	}
	return lhs
}

// MaxClock reduces by taking the largest offset of each common journal.
func MaxClock(lhs, rhs pb.Offsets) pb.Offsets {
	lhs = lhs.Copy()

	for journal, r := range rhs {
		if l, ok := lhs[journal]; !ok {
			lhs[journal] = r
		} else if l < r {
			lhs[journal] = r
		}
	}
	return lhs
}

// ContainsClock returns true if the `rhs` clock is contained within the `lhs`,
// meaning that all `rhs` journals are present in `lhs` with an equal or greater offset.
func ContainsClock(lhs, rhs pb.Offsets) bool {
	for journal, offset := range rhs {
		if lhs[journal] < offset {
			return false
		}
	}
	return true
}
