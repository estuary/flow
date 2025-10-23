package runtime

import (
	"testing"

	pr "github.com/estuary/flow/go/protocols/runtime"
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
