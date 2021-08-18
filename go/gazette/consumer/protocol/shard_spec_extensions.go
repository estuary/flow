package protocol

import (
	"fmt"
	"path"

	pb "go.gazette.dev/core/broker/protocol"
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
	if err := pb.ValidateToken(id.String(), pb.TokenSymbols, minShardNameLen, maxShardNameLen); err != nil {
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
	} else if m.RecoveryLogPrefix != "" && m.RecoveryLog().Validate() != nil {
		return pb.ExtendContext(m.RecoveryLog().Validate(), "RecoveryLogPrefix")
	} else if m.RecoveryLogPrefix == "" && m.HintPrefix != "" {
		return pb.NewValidationError("invalid non-empty HintPrefix with empty RecoveryLogPrefix (%v)", m.HintPrefix)
	} else if m.RecoveryLogPrefix != "" && !isAbsoluteCleanNonDirPath(m.HintPrefix) {
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

	// HotStandbys, Disable, and DisableWaitForAck require no extra validation.

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

// RecoveryLog returns the Journal to which the Shard's recovery log is recorded.
// IF the Shard has no recovery log, "" is returned..
func (m *ShardSpec) RecoveryLog() pb.Journal {
	if m.RecoveryLogPrefix == "" {
		return ""
	}
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

	if a.DisableWaitForAck == false {
		a.DisableWaitForAck = b.DisableWaitForAck
	}
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

	if a.DisableWaitForAck != b.DisableWaitForAck {
		a.DisableWaitForAck = false
	}
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

	if a.DisableWaitForAck == b.DisableWaitForAck {
		a.DisableWaitForAck = false
	}
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

func isAbsoluteCleanNonDirPath(p string) bool {
	return path.IsAbs(p) && path.Clean(p) == p && path.Base(p) != ""
}

const (
	minShardNameLen, maxShardNameLen = 4, 512
)
