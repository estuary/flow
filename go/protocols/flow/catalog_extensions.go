package flow

import (
	bytes "bytes"
	"encoding/json"

	pb "go.gazette.dev/core/broker/protocol"
)

// Task is a common interface of specifications which are also Flow runtime
// tasks. These include CaptureSpec, CollectionSpec, and MaterializationSpec.
type Task interface {
	// TaskName is the catalog name of this task.
	TaskName() string
	// ShardTemplate is the template of this task's ShardSpecs.
	TaskShardTemplate() *ShardSpec
	// RecoveryLogTemplate is the template of this task's JournalSpecs.
	TaskRecoveryLogTemplate() *JournalSpec
}

var _ Task = &CaptureSpec{}
var _ Task = &CollectionSpec{}
var _ Task = &MaterializationSpec{}

// TaskName returns the catalog task name of this capture.
func (m *CaptureSpec) TaskName() string {
	return m.Name.String()
}

// ShardTemplate returns the tasks's shard template.
func (m *CaptureSpec) TaskShardTemplate() *ShardSpec {
	return m.ShardTemplate
}

// RecoveryLogTemplate returns the task's recovery log template.
func (m *CaptureSpec) TaskRecoveryLogTemplate() *JournalSpec {
	return m.RecoveryLogTemplate
}

// TaskName returns the catalog task name of this derivation.
func (m *CollectionSpec) TaskName() string {
	return m.Name.String()
}

// ShardTemplate returns the tasks's shard template.
func (m *CollectionSpec) TaskShardTemplate() *ShardSpec {
	if m.Derivation == nil {
		return nil
	} else {
		return m.Derivation.ShardTemplate
	}
}

// RecoveryLogTemplate returns the task's recovery log template.
func (m *CollectionSpec) TaskRecoveryLogTemplate() *JournalSpec {
	if m.Derivation == nil {
		return nil
	} else {
		return m.Derivation.RecoveryLogTemplate
	}
}

// TaskName returns the catalog task name of this derivation.
func (m *MaterializationSpec) TaskName() string {
	return m.Name.String()
}

// ShardTemplate returns the tasks's shard template.
func (m *MaterializationSpec) TaskShardTemplate() *ShardSpec {
	return m.ShardTemplate
}

// RecoveryLogTemplate returns the task's recovery log template.
func (m *MaterializationSpec) TaskRecoveryLogTemplate() *JournalSpec {
	return m.RecoveryLogTemplate
}

// Validate returns an error if the BuildAPI_Config is malformed.
func (m *BuildAPI_Config) Validate() error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{"BuildId", m.BuildId},
		{"BuildDb", m.BuildDb},
		{"Source", m.Source},
	} {
		if field.value == "" {
			return pb.NewValidationError("missing %s", field.name)
		}
	}

	if _, ok := ContentType_name[int32(m.SourceType)]; !ok {
		return pb.NewValidationError("invalid ContentType %s", m.SourceType)
	}

	return nil
}

// UnmarshalStrict unmarshals |doc| into |m|, using a strict decoding
// of the document which prohibits unknown fields.
// If decoding is successful, then |m| is also validated.
func UnmarshalStrict(doc json.RawMessage, into pb.Validator) error {
	var d = json.NewDecoder(bytes.NewReader(doc))
	d.DisallowUnknownFields()

	if err := d.Decode(into); err != nil {
		return err
	}
	return into.Validate()
}
