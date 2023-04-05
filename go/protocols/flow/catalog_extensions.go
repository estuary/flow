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
	// Shuffles are the shuffles of this task.
	TaskShuffles() []*Shuffle
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
func (m *CollectionSpec) TaskName() string {
	return m.Name.String()
}

// Shuffles returns a *Shuffle for each transform of the derivation.
func (m *CollectionSpec) TaskShuffles() []*Shuffle {
	if m.Derivation == nil {
		return nil
	}

	var shuffles = make([]*Shuffle, len(m.Derivation.Transforms))
	for i := range m.Derivation.Transforms {
		var transform = m.Derivation.Transforms[i]

		var usesSourceKey bool
		var shuffleKey []string
		var shuffleKeyPartitionFields []string

		if len(transform.ShuffleKey) != 0 {
			usesSourceKey = false
			shuffleKey = transform.ShuffleKey
			shuffleKeyPartitionFields = make([]string, len(shuffleKey))

			for i, ptr := range shuffleKey {
				for _, projection := range transform.Collection.Projections {
					if projection.Ptr == ptr && projection.IsPartitionKey {
						shuffleKeyPartitionFields[i] = projection.Field
					}
				}
			}
			for _, field := range shuffleKeyPartitionFields {
				if field == "" {
					shuffleKeyPartitionFields = nil // Not all fields are covered.
				}
			}
		} else if len(transform.ShuffleLambdaConfigJson) != 0 {
			// `shuffleKey` is empty
			usesSourceKey = false
			shuffleKey = nil
			shuffleKeyPartitionFields = nil
		} else {
			usesSourceKey = true
			shuffleKey = transform.Collection.Key
			shuffleKeyPartitionFields = nil
		}

		var validateSchemaJson = transform.Collection.ReadSchemaJson
		if len(validateSchemaJson) == 0 {
			validateSchemaJson = transform.Collection.WriteSchemaJson
		}

		shuffles[i] = &Shuffle{
			GroupName:                 transform.JournalReadSuffix,
			SourceCollection:          transform.Collection.Name,
			SourcePartitions:          transform.PartitionSelector,
			SourceUuidPtr:             transform.Collection.UuidPtr,
			ShuffleKeyPtrs:            shuffleKey,
			ShuffleKeyPartitionFields: shuffleKeyPartitionFields,
			UsesSourceKey:             usesSourceKey,
			FilterRClocks:             transform.ReadOnly,
			ReadDelaySeconds:          transform.ReadDelaySeconds,
			Priority:                  transform.Priority,
			ValidateSchema:            string(validateSchemaJson),
		}
	}
	return shuffles
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

// Shuffles returns a *Shuffle for each binding of the materialization.
func (m *MaterializationSpec) TaskShuffles() []*Shuffle {
	var shuffles = make([]*Shuffle, len(m.Bindings))
	for i := range m.Bindings {
		var binding = m.Bindings[i]

		var partitionSelector = binding.PartitionSelector
		if binding.DeprecatedShuffle != nil {
			partitionSelector = binding.DeprecatedShuffle.PartitionSelector
		}

		var journalReadSuffix = binding.JournalReadSuffix
		if journalReadSuffix == "" {
			journalReadSuffix = binding.DeprecatedShuffle.GroupName
		}

		shuffles[i] = &Shuffle{
			GroupName:                 journalReadSuffix,
			SourceCollection:          binding.Collection.Name,
			SourcePartitions:          partitionSelector,
			SourceUuidPtr:             binding.Collection.UuidPtr,
			ShuffleKeyPtrs:            binding.Collection.Key,
			ShuffleKeyPartitionFields: nil,
			UsesSourceKey:             true,
			FilterRClocks:             false,
			ReadDelaySeconds:          0,
			Priority:                  0,
			ValidateSchema:            "",
		}
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
