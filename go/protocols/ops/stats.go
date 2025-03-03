package ops

import (
	"encoding/json"
	"time"

	"github.com/gogo/protobuf/jsonpb"
)

type jsonStats struct {
	DocsTotal  uint64 `json:"docsTotal,omitempty"`
	BytesTotal uint64 `json:"bytesTotal,omitempty"`
}

// MarshalJSONPB allows us to bypass the protobuf-specific marshalling which would quote uint64
// fields and instead use regular json encoding which does not apply quotes.
func (s *Stats_DocsAndBytes) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	return json.Marshal(jsonStats{
		DocsTotal:  s.DocsTotal,
		BytesTotal: s.BytesTotal,
	})
}

func (s *Stats) GoTimestamp() time.Time {
	return time.Unix(s.Timestamp.Seconds, int64(s.Timestamp.Nanos))
}
