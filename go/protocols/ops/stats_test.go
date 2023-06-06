package ops

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
)

func Test_StatsSerde_RoundTrip(t *testing.T) {
	want := &Stats_DocsAndBytes{
		DocsTotal:            1,
		BytesTotal:           10,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	var buf bytes.Buffer
	require.NoError(t, (&jsonpb.Marshaler{}).Marshal(&buf, want))

	got := new(Stats_DocsAndBytes)
	require.NoError(t, (&jsonpb.Unmarshaler{}).Unmarshal(bytes.NewReader(buf.Bytes()), got))

	require.Equal(t, want, got)

	// Pretty-print a snapshot.
	pp := make(map[string]interface{})
	require.NoError(t, json.Unmarshal(buf.Bytes(), &pp))
	pb, err := json.MarshalIndent(pp, "", "\t")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(pb))
}
