package ops

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/gogo/protobuf/jsonpb"
)

type jsonStats struct {
	DocsTotal  uint32 `json:"docsTotal,omitempty"`
	BytesTotal uint64 `json:"bytesTotal,omitempty"`
}

var _ jsonpb.JSONPBMarshaler = (*Stats_DocsAndBytes)(nil)

// MarshalJSONPB allows us to bypass the protobuf-specific marshalling which would quote uint64
// fields and instead use regular json encoding which does not apply quotes.
func (s *Stats_DocsAndBytes) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	return json.Marshal(jsonStats{
		DocsTotal:  s.DocsTotal,
		BytesTotal: s.BytesTotal,
	})
}

// StatsCollection returns the collection to which stats for the given task name are written.
func StatsCollection(taskName string) pf.Collection {
	return pf.Collection(fmt.Sprintf("ops/%s/stats", strings.Split(taskName, "/")[0]))
}

// ValidateStatsCollection sanity-checks that the given CollectionSpec is appropriate
// for storing instances of Stats documents.
func ValidateStatsCollection(spec *pf.CollectionSpec) error {
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

func (s *Stats) GoTimestamp() time.Time {
	return time.Unix(s.Timestamp.Seconds, int64(s.Timestamp.Nanos))
}
