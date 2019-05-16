// Package shardspace provides mechanisms for mapping a collection of
// ShardSpecs into a minimally-described, semi hierarchical structure, and for
// mapping back again. This is principally useful for tooling over ShardSpecs,
// which must be written to (& read from) Etcd in fully-specified and explicit
// form. Tooling can map ShardSpecs into a Set, apply edits in that semi-
// hierarchical space, and then flatten resulting changes for application
// to the cluster.
package shardspace

import (
	"sort"

	"github.com/LiveRamp/gazette/v2/pkg/consumer"
)

// Set is a collection of Shards, which may share common configuration.
type Set struct {
	// Common ShardSpec configuration of Shards in the Set. When flattened,
	// Shards having zero-valued fields in their spec derive the value from
	// this ShardSpec.
	Common consumer.ShardSpec
	// Shards of the Set, uniquely ordered on ShardSpec.Id.
	Shards []Shard
}

// Shard is a member Shard of a Set.
type Shard struct {
	// Comment is a no-op field which allows tooling to generate and pass-through
	// comments in written YAML output.
	Comment string `yaml:",omitempty"`
	// Delete marks that the Shard should be deleted.
	Delete *bool `yaml:",omitempty"`
	// ShardSpec of the Shard, which may be partial and incomplete. Zero-valued
	// fields of the ShardSpec derive their value from the common Set ShardSpec.
	Spec consumer.ShardSpec `yaml:",omitempty,inline"`
	// Revision of the Shard within Etcd.
	Revision int64 `yaml:",omitempty"`

	patched bool
}

// FromListResponse builds a Set from a ListResponse.
func FromListResponse(resp *consumer.ListResponse) Set {
	var set Set
	for _, s := range resp.Shards {
		set.Shards = append(set.Shards, Shard{
			Spec:     s.Spec,
			Revision: s.ModRevision,
		})
	}
	set.Hoist()
	return set
}

// Hoist specifications of the Set, lifting ShardSpec configuration which is
// common across all Shards of the Set into the Set's common ShardSpec. The field
// is then zeroed at each constituent Shard.
func (s *Set) Hoist() {
	// Build s.ShardSpec as the intersection of every child spec.
	for i, c := range s.Shards {
		if i == 0 {
			s.Common = c.Spec
			s.Common.Id = ""
		} else {
			s.Common = consumer.IntersectShardSpecs(s.Common, c.Spec)
		}
	}
	// Subtract portions of Shards covered by the common ShardSpec.
	for i, c := range s.Shards {
		s.Shards[i].Spec = consumer.SubtractShardSpecs(c.Spec, s.Common)
	}
}

// PushDown specification of the Set, pushing common ShardSpec configuration
// into constituent ShardSpecs having corresponding zero-valued fields.
func (s *Set) PushDown() {
	for i := range s.Shards {
		s.Shards[i].Spec = consumer.UnionShardSpecs(s.Shards[i].Spec, s.Common)
	}
	s.Common = consumer.ShardSpec{}
}

// Patch |shard| into the Set, inserting it if required and otherwise updating
// with fields of |shard| which are not zero-valued. Patch returns a reference to
// the patched *Shard, which may also be inspected and updated directly. However,
// note the returned *Shard is invalidated with the next call to Patch.
func (s *Set) Patch(shard Shard) *Shard {
	var ind = sort.Search(len(s.Shards), func(i int) bool {
		return s.Shards[i].Spec.Id >= shard.Spec.Id
	})
	var found = ind != len(s.Shards) && s.Shards[ind].Spec.Id == shard.Spec.Id

	if !found {
		// Splice in the new |shard|.
		s.Shards = append(s.Shards, Shard{})
		copy(s.Shards[ind+1:], s.Shards[ind:])
		shard.patched = true
		s.Shards[ind] = shard
	} else {
		// Patch in non-zero-valued fields of |shard|.
		s.Shards[ind].Spec = consumer.UnionShardSpecs(shard.Spec, s.Shards[ind].Spec)

		if shard.Delete != nil {
			s.Shards[ind].Delete = shard.Delete
		}
		if shard.Revision != 0 {
			s.Shards[ind].Revision = shard.Revision
		}
		if shard.Comment != "" {
			s.Shards[ind].Comment = shard.Comment
		}
		s.Shards[ind].patched = true
	}
	return &s.Shards[ind]
}

// MarkUnpatchedForDeletion sets Delete for each Shard of the Set
// which has not been Patched.
func (s *Set) MarkUnpatchedForDeletion() {
	var boxedTrue = new(bool)
	*boxedTrue = true

	for i := range s.Shards {
		if s.Shards[i].patched == false {
			s.Shards[i].Delete = boxedTrue
		}
	}
}
