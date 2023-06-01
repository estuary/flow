package ops

import (
	"bytes"
	"fmt"
	"testing"

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
	if err := (&jsonpb.Marshaler{}).Marshal(&buf, want); err != nil {
		panic(fmt.Errorf("marshal of *Stats should always succeed but: %w", err))
	}

	got := new(Stats_DocsAndBytes)
	if err := (&jsonpb.Unmarshaler{}).Unmarshal(bytes.NewReader(buf.Bytes()), got); err != nil {
		panic(err)
	}

	require.Equal(t, want, got)
}
