package protocol

import (
	"testing"
	"time"

	gc "github.com/go-check/check"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/labels"
)

type SpecSuite struct{}

func (s *SpecSuite) TestShardValidationCases(c *gc.C) {
	var cases = []struct {
		s      ShardID
		expect string
	}{
		{"a-valid-shard", ""}, // Success.
		{"not-$%|-a-token", `not a valid token \(.*\)`},
		{"", `invalid length \(0; expected 4 <= .*`},
		{"zz", `invalid length \(2; expected 4 <= .*`},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.s.Validate(), gc.IsNil)
		} else {
			c.Check(tc.s.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *SpecSuite) TestShardSpecValidationCases(c *gc.C) {
	var spec = ShardSpec{
		Id: "bad id",
		Sources: []ShardSpec_Source{
			{Journal: "journal 2"},
			{Journal: "journal/1", MinOffset: -1},
		},
		RecoveryLogPrefix: "bad prefix",
		HintPrefix:        "not empty",
		HintBackups:       -1,
		MaxTxnDuration:    +0,
		MinTxnDuration:    -1,
		LabelSet:          pb.LabelSet{Labels: []pb.Label{{Name: "bad label", Value: "value"}}},
	}

	c.Check(spec.Validate(), gc.ErrorMatches, `Id: not a valid token \(bad id\)`)
	spec.Id = "a-shard-id"
	c.Check(spec.Validate(), gc.ErrorMatches, `RecoveryLogPrefix: not a valid token \(bad prefix/a-shard-id\)`)
	spec.RecoveryLogPrefix = ""
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid non-empty HintPrefix with empty RecoveryLogPrefix \(not empty\)`)
	spec.HintPrefix, spec.RecoveryLogPrefix = "", "recovery/logs"
	c.Check(spec.Validate(), gc.ErrorMatches, `HintPrefix is not an absolute, clean, non-directory path \(\)`)
	spec.HintPrefix = "relative/path"
	c.Check(spec.Validate(), gc.ErrorMatches, `HintPrefix is not an absolute, clean, non-directory path \(relative/path\)`)
	spec.HintPrefix = "/rooted/dir/"
	c.Check(spec.Validate(), gc.ErrorMatches, `HintPrefix is not an absolute, clean, non-directory path \(/rooted/dir/\)`)
	spec.HintPrefix = "/rooted//path" // Not clean.
	c.Check(spec.Validate(), gc.ErrorMatches, `HintPrefix is not an absolute, clean, non-directory path \(/rooted//path\)`)
	spec.HintPrefix = "/rooted/path"
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid HintBackups \(-1; expected >= 0\)`)
	spec.HintBackups = 2
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid MinTxnDuration \(-1; expected >= 0\)`)
	spec.MinTxnDuration = 0
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid MaxTxnDuration \(0; expected > 0\)`)
	spec.MaxTxnDuration = 1
	c.Check(spec.Validate(), gc.ErrorMatches, `LabelSet.Labels\[0\].Name: not a valid token \(bad label\)`)
	spec.LabelSet = pb.MustLabelSet(labels.Instance, "a", labels.Instance, "b")
	c.Check(spec.Validate(), gc.ErrorMatches, `LabelSet: expected single-value Label has multiple values \(index 1; label `+labels.Instance+` value b\)`)
	spec.LabelSet = pb.MustLabelSet("id", "an-id")
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels cannot include label "id"`)
	spec.LabelSet = pb.MustLabelSet("id", "") // Label is rejected even if empty.
	c.Check(spec.Validate(), gc.ErrorMatches, `Labels cannot include label "id"`)
	spec.LabelSet = pb.MustLabelSet(labels.Instance, "an-instance", labels.ManagedBy, "a-tool")

	c.Check(spec.Validate(), gc.ErrorMatches, `Sources\[0\].Journal: not a valid token \(journal 2\)`)
	spec.Sources[0].Journal = "journal/2"
	c.Check(spec.Validate(), gc.ErrorMatches, `Sources\[1\]: invalid MinOffset \(-1; expected > 0\)`)
	spec.Sources[1].MinOffset = 1024
	c.Check(spec.Validate(), gc.ErrorMatches, `Sources.Journal not in unique, sorted order \(index 1; journal/1 <= journal/2\)`)
	spec.Sources[0], spec.Sources[1] = spec.Sources[1], spec.Sources[0]

	c.Check(spec.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestShardSpecRoutines(c *gc.C) {
	var spec = ShardSpec{
		Id:          "shard-id",
		HotStandbys: 2,
		Disable:     false,
		HintPrefix:  "/a/path",
		HintBackups: 2,
	}
	c.Check(spec.DesiredReplication(), gc.Equals, 3)
	spec.Disable = true
	c.Check(spec.DesiredReplication(), gc.Equals, 0)
	spec.Disable, spec.HotStandbys = false, 0
	c.Check(spec.DesiredReplication(), gc.Equals, 1)

	c.Check(ExtractShardSpecMetaLabels(&spec, pb.MustLabelSet("label", "buffer")),
		gc.DeepEquals, pb.MustLabelSet("id", "shard-id"))

	c.Check(spec.HintPrimaryKey(), gc.Equals, "/a/path/shard-id.primary")
	c.Check(spec.HintBackupKeys(), gc.DeepEquals, []string{
		"/a/path/shard-id.backup.0",
		"/a/path/shard-id.backup.1",
	})
}

func (s *SpecSuite) TestSetOperations(c *gc.C) {
	var model = ShardSpec{
		Sources: []ShardSpec_Source{
			{Journal: "a/source", MinOffset: 1234},
		},
		RecoveryLogPrefix: "log/prefix",
		HintPrefix:        "/hints/prefix",
		HintBackups:       3,
		MaxTxnDuration:    5 * time.Second,
		MinTxnDuration:    1 * time.Second,
		Disable:           true,
		HotStandbys:       2,
		LabelSet: pb.LabelSet{
			Labels: []pb.Label{
				{Name: "aaa", Value: "val"},
				{Name: "bbb", Value: "val"},
				{Name: "ccc", Value: "val"},
			},
		},
		DisableWaitForAck: true,
	}
	var other = ShardSpec{
		Sources: []ShardSpec_Source{
			{Journal: "other/source", MinOffset: 5678},
		},
		RecoveryLogPrefix: "other/log/prefix",
		HintPrefix:        "/hints/other/prefix",
		HintBackups:       2,
		MaxTxnDuration:    time.Hour,
		MinTxnDuration:    time.Minute,
		Disable:           false,
		HotStandbys:       1,
		LabelSet: pb.LabelSet{
			Labels: []pb.Label{
				{Name: "aaa", Value: "other"},
				{Name: "bbb", Value: "other"},
				{Name: "ccc", Value: "other"},
			},
		},
		DisableWaitForAck: false,
	}

	c.Check(UnionShardSpecs(ShardSpec{}, model), gc.DeepEquals, model)
	c.Check(UnionShardSpecs(model, ShardSpec{}), gc.DeepEquals, model)

	other.Disable = true // Disable == true dominates in union operation.
	other.DisableWaitForAck = true
	c.Check(UnionShardSpecs(other, model), gc.DeepEquals, other)
	other.Disable = false
	other.DisableWaitForAck = false
	c.Check(UnionShardSpecs(model, other), gc.DeepEquals, model)

	c.Check(IntersectShardSpecs(model, model), gc.DeepEquals, model)
	c.Check(IntersectShardSpecs(model, ShardSpec{}), gc.DeepEquals, ShardSpec{})
	c.Check(IntersectShardSpecs(ShardSpec{}, model), gc.DeepEquals, ShardSpec{})

	c.Check(IntersectShardSpecs(other, model), gc.DeepEquals, ShardSpec{})
	c.Check(IntersectShardSpecs(model, other), gc.DeepEquals, ShardSpec{})

	c.Check(SubtractShardSpecs(model, model), gc.DeepEquals, ShardSpec{})
	c.Check(SubtractShardSpecs(model, ShardSpec{}), gc.DeepEquals, model)
	c.Check(SubtractShardSpecs(ShardSpec{}, model), gc.DeepEquals, ShardSpec{})

	c.Check(SubtractShardSpecs(other, model), gc.DeepEquals, other)
	c.Check(SubtractShardSpecs(model, other), gc.DeepEquals, model)
}

func (s *SpecSuite) TestConsumerSpecValidationCases(c *gc.C) {
	var spec = ConsumerSpec{
		ProcessSpec: pb.ProcessSpec{
			Id:       pb.ProcessSpec_ID{Zone: "not valid", Suffix: "name"},
			Endpoint: "http://foo",
		},
		ShardLimit: 5,
	}
	c.Check(spec.Validate(), gc.ErrorMatches, `Id.Zone: not a valid token \(not valid\)`)
	spec.Id.Zone = "zone"

	c.Check(spec.Validate(), gc.IsNil)
	c.Check(spec.ItemLimit(), gc.Equals, 5)
}

func (s *SpecSuite) TestReplicaStatusValidationCases(c *gc.C) {
	var status = ReplicaStatus{Code: -1}

	c.Check(status.Validate(), gc.ErrorMatches, `Code: invalid code \(-1\)`)
	status.Code = ReplicaStatus_STANDBY
	c.Check(status.Validate(), gc.IsNil)

	status.Errors = []string{"error!"}
	c.Check(status.Validate(), gc.ErrorMatches, `expected Code FAILED with non-empty Errors`)
	status.Errors, status.Code = nil, ReplicaStatus_FAILED
	c.Check(status.Validate(), gc.ErrorMatches, `expected non-empty Errors with Code FAILED`)

	status.Errors = []string{"error!"}
	c.Check(status.Validate(), gc.IsNil)
}

func (s *SpecSuite) TestReplicaStatusReduction(c *gc.C) {
	var status = &ReplicaStatus{Code: ReplicaStatus_IDLE}

	// Code is increased (only) by reducing with a ReplicaStatus of greater Code value.
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_STANDBY})
	c.Check(status, gc.DeepEquals, &ReplicaStatus{Code: ReplicaStatus_STANDBY})
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_BACKFILL})
	c.Check(status, gc.DeepEquals, &ReplicaStatus{Code: ReplicaStatus_STANDBY})

	// Multiple errors are accumulated.
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{"err-1"}})
	status.Reduce(&ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{"err-2"}})
	c.Check(status, gc.DeepEquals, &ReplicaStatus{Code: ReplicaStatus_FAILED, Errors: []string{"err-1", "err-2"}})
}

var _ = gc.Suite(&SpecSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
