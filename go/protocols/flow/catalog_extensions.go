package flow

import (
	bytes "bytes"
	"encoding/json"
	"fmt"
	"path/filepath"

	pb "go.gazette.dev/core/broker/protocol"
)

// Task is a common interface of specifications which are also Flow runtime
// tasks. These include CaptureSpec, DerivationSpec, and MaterializationSpec.
type Task interface {
	// TaskName is the catalog name of this task.
	TaskName() string
	// Shuffles are the shuffles of this task.
	TaskShuffles() []*Shuffle
	// ShardTemplate is the template of this task's ShardSpecs.
	TaskShardTemplate() *ShardSpec
	// RecoveryLogTemplate is the template of this task's JournalSpecs.
	TaskRecoveryLogTemplate() *JournalSpec
}

var _ Task = &CaptureSpec{}
var _ Task = &DerivationSpec{}
var _ Task = &MaterializationSpec{}

// TaskName returns the catalog task name of this capture.
func (m *CaptureSpec) TaskName() string {
	return m.Capture.String()
}

// Shuffles returns a nil slice, as captures have no shuffles.
func (m *CaptureSpec) TaskShuffles() []*Shuffle {
	return nil
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
func (m *DerivationSpec) TaskName() string {
	return m.Collection.Collection.String()
}

// Shuffles returns a *Shuffle for each transform of the derivation.
func (m *DerivationSpec) TaskShuffles() []*Shuffle {
	var shuffles = make([]*Shuffle, len(m.Transforms))
	for i := range m.Transforms {
		shuffles[i] = &m.Transforms[i].Shuffle
	}
	return shuffles
}

// ShardTemplate returns the tasks's shard template.
func (m *DerivationSpec) TaskShardTemplate() *ShardSpec {
	return m.ShardTemplate
}

// RecoveryLogTemplate returns the task's recovery log template.
func (m *DerivationSpec) TaskRecoveryLogTemplate() *JournalSpec {
	return m.RecoveryLogTemplate
}

// TaskName returns the catalog task name of this derivation.
func (m *MaterializationSpec) TaskName() string {
	return m.Materialization.String()
}

// Shuffles returns a *Shuffle for each binding of the materialization.
func (m *MaterializationSpec) TaskShuffles() []*Shuffle {
	var shuffles = make([]*Shuffle, len(m.Bindings))
	for i := range m.Bindings {
		shuffles[i] = &m.Bindings[i].Shuffle
	}
	return shuffles
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
		{"Directory", m.Directory},
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

// OutputPath returns the implied output database path of the build configuration.
func (m *BuildAPI_Config) OutputPath() string {
	return filepath.Join(m.Directory, m.BuildId)
}

func (m LogLevel) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", m.String())), nil
}

func (m *LogLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	} else if v, ok := LogLevel_value[s]; !ok {
		return fmt.Errorf("unrecognized LogLevel %q", s)
	} else {
		*m = LogLevel(v)
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
