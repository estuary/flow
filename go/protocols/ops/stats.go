package ops

import (
	"encoding/json"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
)

// Custom JSON marshaling for stats protobuf types.
//
// The generated protobuf structs have two problems for our JSON output:
//   - jsonpb quotes uint64 fields as strings (e.g. "docsTotal": "5")
//   - The json struct tags use snake_case (e.g. "docs_total") rather than
//     the camelCase our stats schema expects (e.g. "docsTotal")
//
// We fix this by marshaling through shadow structs with correct tags and
// using json.Marshal (which doesn't quote uint64) instead of jsonpb.

// Shadow struct for Stats_DocsAndBytes with camelCase json tags.
type jsonDocsAndBytes struct {
	DocsTotal  uint64 `json:"docsTotal,omitempty"`
	BytesTotal uint64 `json:"bytesTotal,omitempty"`
}

// Shadow struct for Stats_MaterializeBinding with camelCase json tags.
type jsonMaterializeBinding struct {
	Left                  *Stats_DocsAndBytes `json:"left,omitempty"`
	Right                 *Stats_DocsAndBytes `json:"right,omitempty"`
	Out                   *Stats_DocsAndBytes `json:"out,omitempty"`
	LastSourcePublishedAt *time.Time          `json:"lastSourcePublishedAt,omitempty"`
	BytesBehind           uint64              `json:"bytesBehind,omitempty"`
}

// Shadow struct for Stats_CaptureBinding with camelCase json tags.
type jsonCaptureBinding struct {
	Right           *Stats_DocsAndBytes `json:"right,omitempty"`
	Out             *Stats_DocsAndBytes `json:"out,omitempty"`
	LastPublishedAt *time.Time          `json:"lastPublishedAt,omitempty"`
}

// Shadow struct for Stats_Derive_Transform with camelCase json tags.
type jsonDeriveTransform struct {
	Source                string              `json:"source,omitempty"`
	Input                 *Stats_DocsAndBytes `json:"input,omitempty"`
	LastSourcePublishedAt *time.Time          `json:"lastSourcePublishedAt,omitempty"`
	BytesBehind           uint64              `json:"bytesBehind,omitempty"`
}

// MarshalJSONPB is called by jsonpb when serializing Stats_DocsAndBytes directly.
func (s *Stats_DocsAndBytes) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	return json.Marshal(jsonDocsAndBytes{
		DocsTotal:  s.DocsTotal,
		BytesTotal: s.BytesTotal,
	})
}

// MarshalJSON is called by json.Marshal when Stats_DocsAndBytes appears as a
// nested field inside jsonMaterializeBinding (which is serialized via json.Marshal, not jsonpb).
func (s *Stats_DocsAndBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonDocsAndBytes{
		DocsTotal:  s.DocsTotal,
		BytesTotal: s.BytesTotal,
	})
}

// MarshalJSONPB takes over serialization of Stats_MaterializeBinding so that
// BytesBehind is a JSON number. This also requires manually converting
// LastSourcePublishedAt from a protobuf Timestamp to a time.Time, since we
// bypass jsonpb's native Timestamp handling.
func (b *Stats_MaterializeBinding) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	var jb = jsonMaterializeBinding{
		Left:        b.Left,
		Right:       b.Right,
		Out:         b.Out,
		BytesBehind: b.BytesBehind,
	}
	jb.LastSourcePublishedAt = pbTimestampToTime(b.LastSourcePublishedAt)

	return json.Marshal(jb)
}

// MarshalJSONPB takes over serialization of Stats_CaptureBinding so that
// LastPublishedAt is correctly converted from a protobuf Timestamp.
func (b *Stats_CaptureBinding) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	var jb = jsonCaptureBinding{
		Right: b.Right,
		Out:   b.Out,
	}
	jb.LastPublishedAt = pbTimestampToTime(b.LastPublishedAt)

	return json.Marshal(jb)
}

// MarshalJSONPB takes over serialization of Stats_Derive_Transform so that
// BytesBehind is a JSON number. This also requires manually converting
// LastSourcePublishedAt from a protobuf Timestamp to a time.Time, since we
// bypass jsonpb's native Timestamp handling.
func (t *Stats_Derive_Transform) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	var jt = jsonDeriveTransform{
		Source:      t.Source,
		Input:       t.Input,
		BytesBehind: t.BytesBehind,
	}
	jt.LastSourcePublishedAt = pbTimestampToTime(t.LastSourcePublishedAt)

	return json.Marshal(jt)
}

func pbTimestampToTime(ts *types.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	var t = time.Unix(ts.Seconds, int64(ts.Nanos)).UTC()

	return &t
}
