package ops

import (
	"fmt"
	"reflect"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// ValidateLogsCollection sanity-checks that the given CollectionSpec is appropriate
// for storing instances of Log documents.
func ValidateLogsCollection(spec *pf.CollectionSpec) error {
	if !reflect.DeepEqual(
		spec.Key,
		[]string{"/shard/name", "/shard/keyBegin", "/shard/rClockBegin", "/ts"},
	) {
		return fmt.Errorf("CollectionSpec doesn't have expected key: %v", spec.Key)
	}

	if !reflect.DeepEqual(spec.PartitionFields, []string{"kind", "name"}) {
		return fmt.Errorf(
			"CollectionSpec doesn't have expected partitions 'kind' & 'name': %v",
			spec.PartitionFields)
	}

	return nil
}
