package consumer

import (
	"fmt"
	"path"

	"github.com/LiveRamp/gazette/v2/pkg/allocator"
	"github.com/LiveRamp/gazette/v2/pkg/keyspace"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

// ShardID uniquely identifies a shard processed by a Gazette consumer.
type ShardID string

// RoutedShardClient composes a ShardClient and DispatchRouter.
type RoutedShardClient interface {
	ShardClient
	pb.DispatchRouter
}

// NewRoutedShardClient composes a ShardClient and DispatchRouter.
func NewRoutedShardClient(sc ShardClient, dr pb.DispatchRouter) RoutedShardClient {
	return struct {
		ShardClient
		pb.DispatchRouter
	}{sc, dr}
}

// Validate returns an error if the Shard is not well-formed.
func (id ShardID) Validate() error {
	if err := pb.ValidateToken(id.String(), minShardNameLen, maxShardNameLen); err != nil {
		return err
	}
	return nil
}

// String returns the Shard as a string.
func (id ShardID) String() string { return string(id) }

// Validate returns an error if the ShardSpec is not well-formed.
func (m *ShardSpec) Validate() error {
	if err := m.Id.Validate(); err != nil {
		return pb.ExtendContext(err, "Id")
	} else if len(m.Sources) == 0 {
		return pb.NewValidationError("Sources cannot be empty")
	} else if m.RecoveryLogPrefix == "" {
		return pb.NewValidationError("expected RecoveryLogPrefix")
	} else if err = m.RecoveryLog().Validate(); err != nil {
		return pb.ExtendContext(err, "RecoveryLog")
	} else if !path.IsAbs(m.HintPrefix) || path.Clean(m.HintPrefix) != m.HintPrefix || path.Base(m.HintPrefix) == "" {
		return pb.NewValidationError("HintPrefix is not an absolute, clean, non-directory path (%v)", m.HintPrefix)
	} else if m.HintBackups < 0 {
		return pb.NewValidationError("invalid HintBackups (%d; expected >= 0)", m.HintBackups)
	} else if m.MinTxnDuration < 0 {
		return pb.NewValidationError("invalid MinTxnDuration (%d; expected >= 0)", m.MinTxnDuration)
	} else if m.MaxTxnDuration <= 0 {
		return pb.NewValidationError("invalid MaxTxnDuration (%d; expected > 0)", m.MaxTxnDuration)
	} else if err = m.LabelSet.Validate(); err != nil {
		return pb.ExtendContext(err, "LabelSet")
	} else if err = pb.ValidateSingleValueLabels(m.LabelSet); err != nil {
		return pb.ExtendContext(err, "LabelSet")
	} else if len(m.LabelSet.ValuesOf("id")) != 0 {
		return pb.NewValidationError(`Labels cannot include label "id"`)
	}

	for i := range m.Sources {
		if err := m.Sources[i].Validate(); err != nil {
			return pb.ExtendContext(err, "Sources[%d]", i)
		} else if i != 0 && m.Sources[i].Journal <= m.Sources[i-1].Journal {
			return pb.NewValidationError("Sources.Journal not in unique, sorted order (index %d; %+v <= %+v)",
				i, m.Sources[i].Journal, m.Sources[i-1].Journal)
		}
	}

	// Disable and HotStandbys require no extra validation.

	return nil
}

// Validate returns an error if the ShardSpec_Source is not well-formed.
func (m *ShardSpec_Source) Validate() error {
	if err := m.Journal.Validate(); err != nil {
		return pb.ExtendContext(err, "Journal")
	} else if m.MinOffset < 0 {
		return pb.NewValidationError("invalid MinOffset (%d; expected > 0)", m.MinOffset)
	}
	return nil
}

// MarshalString returns the marshaled encoding of the ShardSpec as a string.
func (m *ShardSpec) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// DesiredReplication is the desired number of shard replicas. allocator.ItemValue implementation.
func (m *ShardSpec) DesiredReplication() int {
	if m.Disable {
		return 0
	}
	return 1 + int(m.HotStandbys)
}

// IsConsistent is whether the shard assignment is consistent. allocator.ItemValue implementation.
func (m *ShardSpec) IsConsistent(assignment keyspace.KeyValue, _ keyspace.KeyValues) bool {
	switch assignment.Decoded.(allocator.Assignment).AssignmentValue.(*ReplicaStatus).Code {
	case ReplicaStatus_TAILING, ReplicaStatus_PRIMARY:
		return true
	default:
		return false
	}
}

// RecoveryLog returns the Journal to which the Shard's recoverylog is recorded.
func (m *ShardSpec) RecoveryLog() pb.Journal {
	return pb.Journal(m.RecoveryLogPrefix + "/" + m.Id.String())
}

// HintPrimaryKey returns the Etcd key to which recorded, primary hints are written.
func (m *ShardSpec) HintPrimaryKey() string { return m.HintPrefix + "/" + m.Id.String() + ".primary" }

// HintBackupKeys returns Etcd keys to which verified, disaster-recovery hints are written.
func (m *ShardSpec) HintBackupKeys() []string {
	var keys = make([]string, m.HintBackups)
	for i := int32(0); i != m.HintBackups; i++ {
		keys[i] = fmt.Sprintf("%s/%s.backup.%d", m.HintPrefix, m.Id.String(), i)
	}
	return keys
}

// UnionShardSpecs returns a ShardSpec combining all non-zero-valued fields
// across |a| and |b|. Where both |a| and |b| provide a non-zero value for
// a field, the value of |a| is retained.
func UnionShardSpecs(a, b ShardSpec) ShardSpec {
	if len(a.Sources) == 0 {
		a.Sources = append(a.Sources, b.Sources...)
	}
	if a.RecoveryLogPrefix == "" {
		a.RecoveryLogPrefix = b.RecoveryLogPrefix
	}
	if a.HintPrefix == "" {
		a.HintPrefix = b.HintPrefix
	}
	if a.HintBackups == 0 {
		a.HintBackups = b.HintBackups
	}
	if a.MaxTxnDuration == 0 {
		a.MaxTxnDuration = b.MaxTxnDuration
	}
	if a.MinTxnDuration == 0 {
		a.MinTxnDuration = b.MinTxnDuration
	}
	if a.Disable == false {
		a.Disable = b.Disable
	}
	if a.HotStandbys == 0 {
		a.HotStandbys = b.HotStandbys
	}
	a.LabelSet = pb.UnionLabelSets(a.LabelSet, b.LabelSet, pb.LabelSet{})

	return a
}

// IntersectShardSpecs returns a ShardSpec having a non-zero-valued field
// for each field value which is shared between |a| and |b|.
func IntersectShardSpecs(a, b ShardSpec) ShardSpec {
	if !sourcesEq(a.Sources, b.Sources) {
		a.Sources = nil
	}
	if a.RecoveryLogPrefix != b.RecoveryLogPrefix {
		a.RecoveryLogPrefix = ""
	}
	if a.HintPrefix != b.HintPrefix {
		a.HintPrefix = ""
	}
	if a.HintBackups != b.HintBackups {
		a.HintBackups = 0
	}
	if a.MaxTxnDuration != b.MaxTxnDuration {
		a.MaxTxnDuration = 0
	}
	if a.MinTxnDuration != b.MinTxnDuration {
		a.MinTxnDuration = 0
	}
	if a.Disable != b.Disable {
		a.Disable = false
	}
	if a.HotStandbys != b.HotStandbys {
		a.HotStandbys = 0
	}
	a.LabelSet = pb.IntersectLabelSets(a.LabelSet, b.LabelSet, pb.LabelSet{})

	return a
}

// SubtractShardSpecs returns a ShardSpec derived from |a| but having a
// zero-valued field for each field which is matched by |b|.
func SubtractShardSpecs(a, b ShardSpec) ShardSpec {
	if sourcesEq(a.Sources, b.Sources) {
		a.Sources = nil
	}
	if a.RecoveryLogPrefix == b.RecoveryLogPrefix {
		a.RecoveryLogPrefix = ""
	}
	if a.HintPrefix == b.HintPrefix {
		a.HintPrefix = ""
	}
	if a.HintBackups == b.HintBackups {
		a.HintBackups = 0
	}
	if a.MaxTxnDuration == b.MaxTxnDuration {
		a.MaxTxnDuration = 0
	}
	if a.MinTxnDuration == b.MinTxnDuration {
		a.MinTxnDuration = 0
	}
	if a.Disable == b.Disable {
		a.Disable = false
	}
	if a.HotStandbys == b.HotStandbys {
		a.HotStandbys = 0
	}
	a.LabelSet = pb.SubtractLabelSet(a.LabelSet, b.LabelSet, pb.LabelSet{})

	return a
}

// ExtractShardSpecMetaLabels returns meta-labels of the ShardSpec, using |out| as a buffer.
func ExtractShardSpecMetaLabels(spec *ShardSpec, out pb.LabelSet) pb.LabelSet {
	out.Labels = append(out.Labels[:0], pb.Label{Name: "id", Value: spec.Id.String()})
	return out
}

// Validate returns an error if the ConsumerSpec is not well-formed.
func (m *ConsumerSpec) Validate() error {
	if err := m.ProcessSpec.Validate(); err != nil {
		return err
	}
	// ShardLimit requires no extra validation.
	return nil
}

// MarshalString returns the marshaled encoding of the ConsumerSpec as a string.
func (m *ConsumerSpec) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// ZeroLimit zeros the ConsumerSpec ShardLimit.
func (m *ConsumerSpec) ZeroLimit() { m.ShardLimit = 0 }

// ItemLimit is the maximum number of shards this consumer may process. allocator.MemberValue implementation.
func (m *ConsumerSpec) ItemLimit() int { return int(m.ShardLimit) }

// Reduce folds another ReplicaStatus into this one.
func (m *ReplicaStatus) Reduce(other *ReplicaStatus) {
	if other.Code > m.Code {
		m.Code = other.Code
	}
	for _, e := range other.Errors {
		m.Errors = append(m.Errors, e)
	}
}

// Validate returns an error if the ReplicaStatus is not well-formed.
func (m *ReplicaStatus) Validate() error {
	if err := m.Code.Validate(); err != nil {
		return pb.ExtendContext(err, "Code")
	}

	if len(m.Errors) == 0 {
		if m.Code == ReplicaStatus_FAILED {
			return pb.NewValidationError("expected non-empty Errors with Code FAILED")
		}
	} else if m.Code != ReplicaStatus_FAILED {
		return pb.NewValidationError("expected Code FAILED with non-empty Errors")
	}

	return nil
}

// Validate returns an error if the ReplicaStatus_Code is not well-formed.
func (x ReplicaStatus_Code) Validate() error {
	if _, ok := ReplicaStatus_Code_name[int32(x)]; !ok {
		return pb.NewValidationError("invalid code (%s)", x)
	}
	return nil
}

// MarshalString returns the marshaled encoding of the ReplicaStatus as a string.
func (m *ReplicaStatus) MarshalString() string {
	var d, err = m.Marshal()
	if err != nil {
		panic(err.Error()) // Cannot happen, as we use no custom marshalling.
	}
	return string(d)
}

// Validate returns an error if the Status is not well-formed.
func (x Status) Validate() error {
	if _, ok := Status_name[int32(x)]; !ok {
		return pb.NewValidationError("invalid status (%s)", x)
	}
	return nil
}

// Validate returns an error if the StatRequest is not well-formed.
func (m *StatRequest) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return pb.ExtendContext(err, "Header")
		}
	}
	if err := m.Shard.Validate(); err != nil {
		return pb.ExtendContext(err, "Shard")
	}
	return nil
}

// Validate returns an error if the StatResponse is not well-formed.
func (m *StatResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	}
	for journal, offset := range m.Offsets {
		var err = journal.Validate()
		if err == nil && offset < 0 {
			err = pb.NewValidationError("invalid offset (%d; expected >= 0)", offset)
		}
		if err != nil {
			return pb.ExtendContext(err, "Offsets[%s]", journal)
		}
	}
	return nil
}

// Validate returns an error if the ListRequest is not well-formed.
func (m *ListRequest) Validate() error {
	if err := m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	}
	return nil
}

// Validate returns an error if the ListResponse is not well-formed.
func (m *ListResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	}
	for i, shard := range m.Shards {
		if err := shard.Validate(); err != nil {
			return pb.ExtendContext(err, "Shards[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ListResponse_Shard is not well-formed.
func (m *ListResponse_Shard) Validate() error {
	if err := m.Spec.Validate(); err != nil {
		return pb.ExtendContext(err, "Spec")
	} else if m.ModRevision <= 0 {
		return pb.NewValidationError("invalid ModRevision (%d; expected > 0)", m.ModRevision)
	} else if err = m.Route.Validate(); err != nil {
		return pb.ExtendContext(err, "Route")
	} else if l1, l2 := len(m.Route.Members), len(m.Status); l1 != l2 {
		return pb.NewValidationError("length of Route.Members and Status are not equal (%d vs %d)", l1, l2)
	}
	for i, status := range m.Status {
		if err := status.Validate(); err != nil {
			return pb.ExtendContext(err, "Status[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ApplyRequest is not well-formed.
func (m *ApplyRequest) Validate() error {
	for i, change := range m.Changes {
		if err := change.Validate(); err != nil {
			return pb.ExtendContext(err, "Changes[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the ApplyRequest_Change is not well-formed.
func (m *ApplyRequest_Change) Validate() error {
	if m.Upsert != nil {
		if m.Delete != "" {
			return pb.NewValidationError("both Upsert and Delete are set (expected exactly one)")
		} else if err := m.Upsert.Validate(); err != nil {
			return pb.ExtendContext(err, "Upsert")
		} else if m.ExpectModRevision < 0 {
			return pb.NewValidationError("invalid ExpectModRevision (%d; expected >= 0)", m.ExpectModRevision)
		}
	} else if m.Delete != "" {
		if err := m.Delete.Validate(); err != nil {
			return pb.ExtendContext(err, "Delete")
		} else if m.ExpectModRevision <= 0 {
			return pb.NewValidationError("invalid ExpectModRevision (%d; expected > 0)", m.ExpectModRevision)
		}
	} else {
		return pb.NewValidationError("neither Upsert nor Delete are set (expected exactly one)")
	}
	return nil
}

// Validate returns an error if the ApplyResponse is not well-formed.
func (m *ApplyResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	} else if err = m.Header.Validate(); err != nil {
		return pb.ExtendContext(err, "Header")
	}
	return nil
}

// Validate returns an error if the HintsRequest is not well-formed.
func (m *GetHintsRequest) Validate() error {
	if err := m.Shard.Validate(); err != nil {
		return pb.ExtendContext(err, "Shard")
	}
	return nil
}

// Validate returns an error if the HintsResponse is not well-formed.
func (m *GetHintsResponse) Validate() error {
	if err := m.Status.Validate(); err != nil {
		return pb.ExtendContext(err, "Status")
	}

	if err := m.PrimaryHints.Validate(); err != nil {
		return pb.ExtendContext(err, "primary hints")
	}
	for _, hints := range m.BackupHints {
		if err := hints.Validate(); err != nil {
			return pb.ExtendContext(err, "backup hints")
		}
	}
	return nil
}

// Validate returns an error if the GetHintsResponse_ResponseHints is not well-formed.
func (m GetHintsResponse_ResponseHints) Validate() error {
	if m.Hints != nil {
		return m.Hints.Validate()
	}
	return nil
}

func sourcesEq(a, b []ShardSpec_Source) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

const (
	minShardNameLen, maxShardNameLen = 4, 512
)
