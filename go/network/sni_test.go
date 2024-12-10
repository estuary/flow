package network

import (
	"testing"

	"github.com/estuary/flow/go/labels"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestResolveSNIMapping(t *testing.T) {
	var (
		parsed = parsedSNI{
			hostname: "abcdefg",
			port:     "8080",
		}
		shard = &pc.ShardSpec{
			Id: "capture/AcmeCo/My/Capture/source-http-ingest/0f05593ad1800023/00000000-00000000",
			LabelSet: pb.MustLabelSet(
				labels.PortProtoPrefix+"8080", "leet",
				labels.PortPublicPrefix+"8080", "true",
			),
		}
	)
	require.Equal(t, resolvedSNI{
		shardIDPrefix: "capture/AcmeCo/My/Capture/source-http-ingest/",
		portProtocol:  "leet",
		portIsPublic:  true,
	}, newResolvedSNI(parsed, shard))
}
