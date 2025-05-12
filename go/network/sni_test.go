package network

import (
	"fmt"
	"testing"

	"github.com/estuary/flow/go/labels"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestParseSNI(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		expect    parsedSNI
		expectErr string // Expected error string, empty if no error
	}{
		{
			name:  "valid two-part SNI",
			input: "hostname123-8080",
			expect: parsedSNI{
				hostname: "hostname123",
				port:     "8080",
			},
			expectErr: "",
		},
		{
			name:  "valid four-part SNI",
			input: "hostxyz-keybegin-rclockbegin-443",
			expect: parsedSNI{
				hostname:    "hostxyz",
				port:        "443",
				keyBegin:    "keybegin",
				rClockBegin: "rclockbegin",
			},
			expectErr: "",
		},
		{
			name:      "invalid SNI - too few parts",
			input:     "hostnameonly",
			expectErr: "expected two or four subdomain components, not 1",
		},
		{
			name:      "invalid SNI - too many parts",
			input:     "a-b-c-d-e",
			expectErr: "expected two or four subdomain components, not 5",
		},
		{
			name:      "invalid SNI - three parts",
			input:     "a-b-c",
			expectErr: "expected two or four subdomain components, not 3",
		},
		{
			name:      "invalid SNI - non-numeric port",
			input:     "hostname123-portabc",
			expectErr: "failed to parse subdomain port number",
		},
		{
			name:      "invalid SNI - non-numeric port in four-part",
			input:     "hostname123-key-rclock-portabc",
			expectErr: "failed to parse subdomain port number",
		},
		{
			name:      "invalid two-part SNI - empty hostname",
			input:     "-12345",
			expectErr: "hostname is empty",
		},
		{
			name:      "invalid four-part SNI - empty keyBegin",
			input:     "h---1",
			expectErr: "keyBegin is empty",
		},
		{
			name:      "invalid four-part SNI - empty hostname",
			input:     "-key1-rclock1-123",
			expectErr: "hostname is empty",
		},
		{
			name:      "invalid four-part SNI - empty rClockBegin",
			input:     "host1-key1--123",
			expectErr: "rClockBegin is empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseSNI(tc.input)

			if tc.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expect, parsed)
				// Test round-tripping
				require.Equal(t, tc.input, parsed.String(), fmt.Sprintf("String() representation mismatch for input: %s", tc.input))
			}
		})
	}
}

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
