package ops

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// Log is the canonical shape of a Flow operations Log document.
// See also:
// * ops-catalog/ops-log-schema.json
// * crate/ops/lib.rs
type Log struct {
	Meta struct {
		UUID string `json:"uuid"`
	} `json:"_meta"`
	Timestamp time.Time       `json:"ts"`
	Level     pf.LogLevel     `json:"level"`
	Message   string          `json:"message"`
	Fields    json.RawMessage `json:"fields,omitempty"`
	Shard     ShardRef        `json:"shard,omitempty"`
	Spans     []Log           `json:"spans,omitempty"`
}

// LogCollection returns the collection to which logs of the given shard are written.
func LogCollection(taskName string) pf.Collection {
	return pf.Collection(fmt.Sprintf("ops/%s/logs", strings.Split(taskName, "/")[0]))
}

// ValidateLogsCollection sanity-checks that the given CollectionSpec is appropriate
// for storing instances of Log documents.
func ValidateLogsCollection(spec *pf.CollectionSpec) error {
	if !reflect.DeepEqual(
		spec.KeyPtrs,
		[]string{"/shard/name", "/shard/keyBegin", "/shard/rClockBegin", "/ts"},
	) {
		return fmt.Errorf("CollectionSpec doesn't have expected key: %v", spec.KeyPtrs)
	}

	if !reflect.DeepEqual(spec.PartitionFields, []string{"kind", "name"}) {
		return fmt.Errorf(
			"CollectionSpec doesn't have expected partitions 'kind' & 'name': %v",
			spec.PartitionFields)
	}

	return nil
}
