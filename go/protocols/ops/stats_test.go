package ops

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func Test_MaterializeBinding_RoundTrip(t *testing.T) {
	var want = &Stats_MaterializeBinding{
		Left:  &Stats_DocsAndBytes{DocsTotal: 3, BytesTotal: 30},
		Right: &Stats_DocsAndBytes{DocsTotal: 5, BytesTotal: 50},
		Out:   &Stats_DocsAndBytes{DocsTotal: 4, BytesTotal: 40},
		LastSourcePublishedAt: &types.Timestamp{
			Seconds: 1700000000,
			Nanos:   123000000,
		},
		BytesBehind: 9999,
	}

	var buf bytes.Buffer
	require.NoError(t, (&jsonpb.Marshaler{}).Marshal(&buf, want))

	var got = new(Stats_MaterializeBinding)
	require.NoError(t, (&jsonpb.Unmarshaler{}).Unmarshal(bytes.NewReader(buf.Bytes()), got))

	require.Equal(t, want, got)

	var pp = make(map[string]any)
	require.NoError(t, json.Unmarshal(buf.Bytes(), &pp))
	var pb, err = json.MarshalIndent(pp, "", "\t")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(pb))
}

func Test_DeriveTransform_RoundTrip(t *testing.T) {
	var want = &Stats_Derive_Transform{
		Source: "acmeCo/orders",
		Input:  &Stats_DocsAndBytes{DocsTotal: 10, BytesTotal: 100},
		LastSourcePublishedAt: &types.Timestamp{
			Seconds: 1700000000,
			Nanos:   789000000,
		},
		BytesBehind: 5432,
	}

	var buf bytes.Buffer
	require.NoError(t, (&jsonpb.Marshaler{}).Marshal(&buf, want))

	var got = new(Stats_Derive_Transform)
	require.NoError(t, (&jsonpb.Unmarshaler{}).Unmarshal(bytes.NewReader(buf.Bytes()), got))

	require.Equal(t, want, got)

	var pp = make(map[string]any)
	require.NoError(t, json.Unmarshal(buf.Bytes(), &pp))
	var pb, err = json.MarshalIndent(pp, "", "\t")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(pb))
}

func Test_CaptureBinding_RoundTrip(t *testing.T) {
	var want = &Stats_CaptureBinding{
		Right: &Stats_DocsAndBytes{DocsTotal: 7, BytesTotal: 70},
		Out:   &Stats_DocsAndBytes{DocsTotal: 6, BytesTotal: 60},
		LastPublishedAt: &types.Timestamp{
			Seconds: 1700000000,
			Nanos:   456000000,
		},
	}

	var buf bytes.Buffer
	require.NoError(t, (&jsonpb.Marshaler{}).Marshal(&buf, want))

	var got = new(Stats_CaptureBinding)
	require.NoError(t, (&jsonpb.Unmarshaler{}).Unmarshal(bytes.NewReader(buf.Bytes()), got))

	require.Equal(t, want, got)

	var pp = make(map[string]any)
	require.NoError(t, json.Unmarshal(buf.Bytes(), &pp))
	var pb, err = json.MarshalIndent(pp, "", "\t")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(pb))
}
