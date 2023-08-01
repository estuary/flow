package shuffle

import (
	"encoding/json"
	"reflect"

	pf "github.com/estuary/flow/go/protocols/flow"
	pr "github.com/estuary/flow/go/protocols/runtime"
)

// shuffle is an internal description of a source shuffle.
// It's not intended to be used outside of the `shuffle` package,
// or to be persisted / serialized, and its layout and semantics
// may change.
type shuffle struct {
	// Should documents be filtered on their r-clocks?
	// This is only true for read-only derivation transforms.
	filterRClocks bool
	// Path metadata suffix attached to journals read by this shuffle.
	journalReadSuffix string
	// Priority of this shuffle with respect to others of the derivation.
	priority uint32
	// Projections of the shuffled source collection.
	projections []pf.Projection
	// Read delay of this shuffle with respect to others of the derivation.
	readDelaySeconds uint32
	// Key of this shuffle. If empty, then `usesLambda` is true.
	shuffleKey []string
	// Partitioned projection fields which fully cover the shuffle key.
	shuffleKeyPartitionFields []string
	// Name of the sourced collection.
	sourceCollection pf.Collection
	// Partition selector of the sourced collection.
	sourcePartitions pf.LabelSelector
	// JSON pointer of source document UUIDs.
	sourceUuidPtr string
	// Shuffle key is dynamically computed from derivation transform source documents.
	usesLambda bool
	// Shuffle key is the same as the source collection key.
	usesSourceKey bool
	// If non-nil, validate the schema on read.
	validateSchema json.RawMessage
}

func derivationShuffles(task *pf.CollectionSpec) []shuffle {
	if task.Derivation == nil {
		return nil
	}
	var shuffles = make([]shuffle, len(task.Derivation.Transforms))

	for i := range task.Derivation.Transforms {
		var transform = task.Derivation.Transforms[i]

		var shuffle = shuffle{
			filterRClocks:             transform.ReadOnly,
			journalReadSuffix:         transform.JournalReadSuffix,
			priority:                  transform.Priority,
			projections:               transform.Collection.Projections,
			readDelaySeconds:          transform.ReadDelaySeconds,
			shuffleKey:                nil,
			shuffleKeyPartitionFields: nil,
			sourceCollection:          transform.Collection.Name,
			sourcePartitions:          transform.PartitionSelector,
			sourceUuidPtr:             transform.Collection.UuidPtr,
			usesLambda:                false,
			usesSourceKey:             false,
			validateSchema:            transform.Collection.ReadSchemaJson,
		}

		// We always validate derivation sources on read,
		// preferring a read schema and falling back to its singular schema.
		if len(shuffle.validateSchema) == 0 {
			shuffle.validateSchema = transform.Collection.WriteSchemaJson
		}

		if len(transform.ShuffleKey) != 0 {
			shuffle.shuffleKey = transform.ShuffleKey
			shuffle.shuffleKeyPartitionFields = make([]string, len(transform.ShuffleKey))
			shuffle.usesSourceKey = reflect.DeepEqual(transform.ShuffleKey, transform.Collection.Key)

			for i, ptr := range transform.ShuffleKey {
				for _, projection := range transform.Collection.Projections {
					if projection.Ptr == ptr && projection.IsPartitionKey {
						shuffle.shuffleKeyPartitionFields[i] = projection.Field
					}
				}
			}
			for _, field := range shuffle.shuffleKeyPartitionFields {
				if field == "" {
					shuffle.shuffleKeyPartitionFields = nil // Not all fields are covered.
				}
			}
		} else if len(transform.ShuffleLambdaConfigJson) != 0 {
			shuffle.usesLambda = true
		} else {
			// Shuffle is `any`. Currently we shuffle on the source key
			// but this is arbitrary and can be changed.
			shuffle.shuffleKey = transform.Collection.Key
			shuffle.usesSourceKey = true
		}

		shuffles[i] = shuffle
	}
	return shuffles
}

func materializationShuffles(task *pf.MaterializationSpec) []shuffle {
	var sources = make([]shuffle, len(task.Bindings))

	for i := range task.Bindings {
		var binding = task.Bindings[i]

		var shuffle = shuffle{
			filterRClocks:             false,
			journalReadSuffix:         binding.JournalReadSuffix,
			priority:                  binding.Priority,
			projections:               binding.Collection.Projections,
			readDelaySeconds:          0,
			shuffleKey:                binding.Collection.Key,
			shuffleKeyPartitionFields: nil,
			sourceCollection:          binding.Collection.Name,
			sourcePartitions:          binding.PartitionSelector,
			sourceUuidPtr:             binding.Collection.UuidPtr,
			usesLambda:                false,
			usesSourceKey:             true,
			validateSchema:            nil,
		}

		// Migration support for materializations built prior to April 2023.
		// TODO(johnny): Remove when all materialization builds have been refreshed.
		if binding.DeprecatedShuffle != nil {
			shuffle.sourcePartitions = binding.DeprecatedShuffle.PartitionSelector
		}
		if shuffle.journalReadSuffix == "" {
			shuffle.journalReadSuffix = binding.DeprecatedShuffle.GroupName
		}

		sources[i] = shuffle
	}
	return sources
}

func requestShuffle(req *pr.ShuffleRequest) shuffle {
	var shuffles []shuffle

	if req.Derivation != nil {
		shuffles = derivationShuffles(req.Derivation)
	} else if req.Materialization != nil {
		shuffles = materializationShuffles(req.Materialization)
	} else {
		panic("must have derivation or materialization")
	}
	return shuffles[req.ShuffleIndex]
}
