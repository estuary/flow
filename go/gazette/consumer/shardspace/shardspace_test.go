package shardspace

import (
	"bytes"
	"strings"
	"testing"
	"time"

	gc "github.com/go-check/check"
	"go.gazette.dev/core/consumer"
	pb "go.gazette.dev/core/protocol"
	"gopkg.in/yaml.v2"
)

type SetSuite struct{}

func (s *SetSuite) TestRoundTripHoistAndPushDown(c *gc.C) {
	var set = buildFlatFixture()
	set.Hoist()

	// Expect shared field values are hoisted to the common ShardSpec.
	// Differentiated fields remain with individual Shards.
	c.Check(set, gc.DeepEquals, Set{
		Common: consumer.ShardSpec{
			RecoveryLogPrefix: "recovery/logs",
			HintPrefix:        "/hints",
			HintBackups:       2,
			MaxTxnDuration:    time.Second,
			MinTxnDuration:    time.Millisecond,
			HotStandbys:       1,
			LabelSet:          pb.MustLabelSet("name-1", "val-1"),
		},
		Shards: []Shard{
			{
				Spec: consumer.ShardSpec{
					Id:       "shard-A",
					Sources:  []consumer.ShardSpec_Source{{Journal: "journal/A"}},
					LabelSet: pb.MustLabelSet("name-2", "val-2"),
				},
				Delete:   &boxedTrue,
				Revision: 123,
				Comment:  "This is a comment",
			},
			{
				Spec: consumer.ShardSpec{
					Id:       "shard-B",
					Sources:  []consumer.ShardSpec_Source{{Journal: "journal/B"}},
					LabelSet: pb.MustLabelSet("name-3", "val-3"),
				},
				Revision: 456,
			},
		},
	})

	// After flattening, expect we recover our initial flat fixture.
	set.PushDown()
	c.Check(set, gc.DeepEquals, buildFlatFixture())
}

func (s *SetSuite) TestPatchAndMarkForDeletion(c *gc.C) {
	var set = buildFlatFixture()
	set.Hoist()

	// Add new shard-C.
	_ = set.Patch(Shard{Spec: consumer.ShardSpec{
		Id:                "shard-C",
		RecoveryLogPrefix: "foo/bar",
	}})
	// Un-delete shard-A. Mix patched fields, with a post-patch update.
	set.Patch(Shard{
		Spec: consumer.ShardSpec{
			Id:      "shard-A",
			Disable: true,
		},
		Delete:   &boxedFalse,
		Revision: 789,
	}).Comment = "foobar"

	// Update shard-C with a new journal source.
	_ = set.Patch(Shard{
		Spec: consumer.ShardSpec{
			Id:      "shard-C",
			Sources: []consumer.ShardSpec_Source{{Journal: "journal/C"}},
		},
	})

	// Expect shard-C is present with its combined updates.
	c.Check(set.Shards, gc.HasLen, 3)
	c.Check(set.Shards[2].Spec.Id, gc.Equals, consumer.ShardID("shard-C"))
	c.Check(set.Shards[2].Spec.Sources, gc.DeepEquals, []consumer.ShardSpec_Source{{Journal: "journal/C"}})
	c.Check(set.Shards[2].Spec.RecoveryLogPrefix, gc.Equals, "foo/bar")

	// Expect shard-A was updated.
	c.Check(set.Shards[0].Spec.Disable, gc.Equals, true)
	c.Check(set.Shards[0].Delete, gc.DeepEquals, &boxedFalse)
	c.Check(set.Shards[0].Comment, gc.Equals, "foobar")
	c.Check(set.Shards[0].Revision, gc.Equals, int64(789))

	// Walk Shards and mark those not Patched, for deletion.
	set.MarkUnpatchedForDeletion()

	c.Check(set.Shards[0].Delete, gc.DeepEquals, &boxedFalse)
	c.Check(set.Shards[1].Delete, gc.DeepEquals, &boxedTrue) // Not visited.
	c.Check(set.Shards[2].Delete, gc.IsNil)
}

func (s *SetSuite) TestYAMLRoundTrip(c *gc.C) {
	const yamlFixture = `
common:
  recovery_log_prefix: recovery/logs
  hint_prefix: /hints
  hint_backups: 2
  max_txn_duration: 1s
  min_txn_duration: 1ms
  hot_standbys: 1
  labels:
  - name: name-1
    value: val-1
shards:
- comment: This is a comment
  delete: true
  id: shard-A
  sources:
  - journal: journal/A
  labels:
  - name: name-2
    value: val-2
  revision: 123
- id: shard-B
  sources:
  - journal: journal/B
  labels:
  - name: name-3
    value: val-3
  revision: 456
`
	var (
		dec = yaml.NewDecoder(strings.NewReader(yamlFixture))
		set Set
	)
	dec.SetStrict(true)
	c.Check(dec.Decode(&set), gc.IsNil)

	set.PushDown()
	c.Check(set, gc.DeepEquals, buildFlatFixture())
	set.Hoist()

	var (
		bw  bytes.Buffer
		enc = yaml.NewEncoder(&bw)
	)
	c.Check(enc.Encode(&set), gc.IsNil)

	c.Check(bw.String(), gc.Equals, yamlFixture[1:])
}

func buildFlatFixture() Set {
	return Set{
		Shards: []Shard{
			{
				Spec: consumer.ShardSpec{
					Id:                "shard-A",
					Sources:           []consumer.ShardSpec_Source{{Journal: "journal/A"}},
					RecoveryLogPrefix: "recovery/logs",
					HintPrefix:        "/hints",
					HintBackups:       2,
					MaxTxnDuration:    time.Second,
					MinTxnDuration:    time.Millisecond,
					HotStandbys:       1,
					LabelSet:          pb.MustLabelSet("name-1", "val-1", "name-2", "val-2"),
				},
				Revision: 123,
				Delete:   &boxedTrue,
				Comment:  "This is a comment",
			},
			{
				Spec: consumer.ShardSpec{
					Id:                "shard-B",
					Sources:           []consumer.ShardSpec_Source{{Journal: "journal/B"}},
					RecoveryLogPrefix: "recovery/logs",
					HintPrefix:        "/hints",
					HintBackups:       2,
					MaxTxnDuration:    time.Second,
					MinTxnDuration:    time.Millisecond,
					HotStandbys:       1,
					LabelSet:          pb.MustLabelSet("name-1", "val-1", "name-3", "val-3"),
				},
				Revision: 456,
			},
		},
	}
}

var (
	_          = gc.Suite(&SetSuite{})
	boxedTrue  = true
	boxedFalse = false
)

func Test(t *testing.T) { gc.TestingT(t) }
