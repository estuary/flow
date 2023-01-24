package ops

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	//"github.com/estuary/flow/go/flow"
	//"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// TODO(johnny): The canonical Go stats document should be
// moved from the runtime package, to here.

// StatsCollection returns the collection to which stats for the given task name are written.
func StatsCollection(taskName string) pf.Collection {
	return pf.Collection(fmt.Sprintf("ops/%s/stats", strings.Split(taskName, "/")[0]))
}

// ValidateStatsCollection sanity-checks that the given CollectionSpec is appropriate
// for storing instances of Stats documents.
func ValidateStatsCollection(spec *pf.CollectionSpec) error {
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

// StatsEvent is the Go struct corresponding to ops/<tenant>/stats collections. It must be
// consistent with the JSON schema: crates/build/src/ops/ops-stats-schema.json
// Many of the types within here closely resemble definitions from flow.proto,
// but we avoid re-using the proto definitions to allow this file to control the json
// representation, and to have more clarity and strictness about which fields are required.
type StatsEvent struct {
	Meta             Meta                               `json:"_meta"`
	Shard            ShardRef                           `json:"shard"`
	Timestamp        time.Time                          `json:"ts"`
	TxnCount         uint64                             `json:"txnCount"`
	OpenSecondsTotal float64                            `json:"openSecondsTotal"`
	Capture          map[string]CaptureBindingStats     `json:"capture,omitempty"`
	Materialize      map[string]MaterializeBindingStats `json:"materialize,omitempty"`
	Derive           *DeriveStats                       `json:"derive,omitempty"`
}

type Meta struct {
	UUID string `json:"uuid"`
}

type DocsAndBytes struct {
	Docs  uint64 `json:"docsTotal"`
	Bytes uint64 `json:"bytesTotal"`
}

// With adds the given proto DocsAndBytes to this one and returns the result.
func (s *DocsAndBytes) With(proto *pf.DocsAndBytes) DocsAndBytes {
	return DocsAndBytes{
		Docs:  s.Docs + uint64(proto.Docs),
		Bytes: s.Bytes + uint64(proto.Bytes),
	}
}

func DocsAndBytesFromProto(proto *pf.DocsAndBytes) DocsAndBytes {
	if proto == nil {
		return DocsAndBytes{}
	}
	return DocsAndBytes{
		Docs:  uint64(proto.Docs),
		Bytes: uint64(proto.Bytes),
	}
}

type CaptureBindingStats struct {
	Right DocsAndBytes `json:"right"`
	Out   DocsAndBytes `json:"out"`
}

type MaterializeBindingStats struct {
	Left  DocsAndBytes `json:"left"`
	Right DocsAndBytes `json:"right"`
	Out   DocsAndBytes `json:"out"`
}

type InvokeStats struct {
	Out          DocsAndBytes `json:"out"`
	SecondsTotal float64      `json:"secondsTotal"`
}

type DeriveTransformStats struct {
	// Source collection for this transform.
	Source string       `json:"source"`
	Input  DocsAndBytes `json:"input"`
	// At least one of Update or Publish must be present in the output,
	// but either one can be optional. This is to avoid outputting zeroed out invocation stats for
	// lambdas that the user hasn't defined.
	Update  *InvokeStats `json:"update,omitempty"`
	Publish *InvokeStats `json:"publish,omitempty"`
}

type DeriveRegisterStats struct {
	CreatedTotal uint64 `json:"createdTotal"`
}

type DeriveStats struct {
	Transforms map[string]DeriveTransformStats `json:"transforms"`
	Out        DocsAndBytes                    `json:"out"`
	Registers  *DeriveRegisterStats            `json:"registers,omitempty"`
}
