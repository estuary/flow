package testing

import (
	pb "go.gazette.dev/core/broker/protocol"
)

// Clock is vector clock of read or write progress, where the current Etcd revision
// and each journal offset are viewed as vectorized Clock components.
type Clock struct {
	// Etcd header obtained from a ingestion or shard Stat RPC.
	// Header revisions can be compared to determine relative "happened before"
	// relationships of readers & writers: if a write dynamically creates a new journal,
	// it must necessarily increase the Etcd revision. From there, we can determine whether
	// downstream transforms must be aware of the new journal (or may not be) by
	// comparing against the last revision Stat'd from their shards.
	Etcd pb.Header_Etcd
	// Offsets are a bag of Gazette journals and a byte offset therein.
	Offsets pb.Offsets
}

// ReduceMin updates this Clock with the minimum of Etcd revision and offset for each journal.
func (c *Clock) ReduceMin(rhsEtcd pb.Header_Etcd, rhsOffsets pb.Offsets) {
	// Take a smaller LHS revision, so long as it's not zero (uninitialized).
	if c.Etcd.Revision == 0 || c.Etcd.Revision > rhsEtcd.Revision {
		c.Etcd = rhsEtcd
	}

	if c.Offsets == nil {
		c.Offsets = make(pb.Offsets)
	}

	// Take the smallest of each common offset.
	for journal, rhs := range rhsOffsets {
		if lhs, ok := c.Offsets[journal]; !ok {
			c.Offsets[journal] = rhs
		} else if lhs > rhs {
			c.Offsets[journal] = rhs
		}
	}
}

// ReduceMax updates this Clock with the maximum of Etcd revision and offset for each journal.
func (c *Clock) ReduceMax(rhsEtcd pb.Header_Etcd, rhsOffsets pb.Offsets) {
	// Take a larger LHS revision.
	if c.Etcd.Revision < rhsEtcd.Revision {
		c.Etcd = rhsEtcd
	}

	if c.Offsets == nil {
		c.Offsets = make(pb.Offsets)
	}

	// Take the largest of each common offset.
	for journal, rhs := range rhsOffsets {
		if lhs, ok := c.Offsets[journal]; !ok {
			c.Offsets[journal] = rhs
		} else if lhs < rhs {
			c.Offsets[journal] = rhs
		}
	}
}

// Contains is true if the Etcd revision of this Clock is greater or equal to the
// revision of the |other|, and if all common journal offsets are also greater or
// equal to |other|.
func (c *Clock) Contains(other *Clock) bool {
	if c.Etcd.Revision < other.Etcd.Revision {
		return false
	}
	for journal, lhs := range c.Offsets {
		if rhs, ok := other.Offsets[journal]; ok && lhs < rhs {
			return false
		}
	}
	return true
}

// Copy returns a deep copy of this Clock.
func (c *Clock) Copy() *Clock {
	return &Clock{
		Etcd:    c.Etcd,
		Offsets: c.Offsets.Copy(),
	}
}
