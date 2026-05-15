package runtime

import (
	"testing"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pr "github.com/estuary/flow/go/protocols/runtime"
	pb "go.gazette.dev/core/broker/protocol"
	"github.com/stretchr/testify/require"
)

func TestFlowConsumerConfig_Plane(t *testing.T) {
	tests := []struct {
		name            string
		allowLocal      bool
		dataplaneFQDN   string
		expectedPlane   pr.Plane
	}{
		{
			name:          "AllowLocal returns LOCAL",
			allowLocal:    true,
			dataplaneFQDN: "aws-eu-west-1-c1.dp.estuary-data.com",
			expectedPlane: pr.Plane_LOCAL,
		},
		{
			name:          "Private data plane matches 16 hex char pattern",
			allowLocal:    false,
			dataplaneFQDN: "f7002c61f85f2b5e.dp.estuary-data.com",
			expectedPlane: pr.Plane_PRIVATE,
		},
		{
			name:          "Public data plane with human-readable FQDN",
			allowLocal:    false,
			dataplaneFQDN: "aws-eu-west-1-c1.dp.estuary-data.com",
			expectedPlane: pr.Plane_PUBLIC,
		},
		{
			name:          "Public data plane defaults when FQDN is empty",
			allowLocal:    false,
			dataplaneFQDN: "",
			expectedPlane: pr.Plane_PUBLIC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config = FlowConsumerConfig{}
			config.Flow.AllowLocal = tt.allowLocal
			config.Flow.DataPlaneFQDN = tt.dataplaneFQDN

			var result = config.Plane()

			require.Equal(t, tt.expectedPlane, result)
		})
	}
}

func TestUseRuntimeV2(t *testing.T) {
	tests := []struct {
		name   string
		labels []pb.Label
		want   bool
	}{
		{
			name:   "no flag",
			labels: []pb.Label{{Name: labels.TaskName, Value: "task"}},
			want:   false,
		},
		{
			name:   "enable-runtime-v2=true",
			labels: []pb.Label{{Name: labels.FlagPrefix + "enable-runtime-v2", Value: "true"}},
			want:   true,
		},
		{
			name:   "enable-runtime-v2=false",
			labels: []pb.Label{{Name: labels.FlagPrefix + "enable-runtime-v2", Value: "false"}},
			want:   false,
		},
		{
			name:   "unrelated flag",
			labels: []pb.Label{{Name: labels.FlagPrefix + "some-other-flag", Value: "true"}},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var set = pf.LabelSet{Labels: tt.labels}
			require.Equal(t, tt.want, useRuntimeV2(set))
		})
	}
}

func TestSidecarEndpoint(t *testing.T) {
	var config = FlowConsumerConfig{}
	config.Flow.SidecarPort = 9100

	got, err := config.SidecarEndpoint(pb.Endpoint("https://reactor-foo.flow.localhost:8080"))
	require.NoError(t, err)
	require.Equal(t, "https://reactor-foo.flow.localhost:9100", got)

	got, err = config.SidecarEndpoint(pb.Endpoint("http://10.0.0.5:8080"))
	require.NoError(t, err)
	require.Equal(t, "http://10.0.0.5:9100", got)

	// No port configured: error.
	config.Flow.SidecarPort = 0
	_, err = config.SidecarEndpoint(pb.Endpoint("https://reactor-foo.flow.localhost:8080"))
	require.ErrorContains(t, err, "sidecar-port")
}
