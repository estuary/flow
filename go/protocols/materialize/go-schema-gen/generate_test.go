package schemagen

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	Password string `json:"password" jsonschema:"title=Password,description=Secret password." jsonschema_extras:"secret=true,order=1"`
	Username string `json:"username" jsonschema:"title=Username,description=Test user." jsonschema_extras:"order=0"`
	Advanced struct {
		LongAdvanced          string `json:"long_advanced,omitempty" jsonschema:"title=Example,description=Some long description." jsonschema_extras:"multiline=true"`
		SecretOrderedAdvanced string `json:"secret_advanced,omitempty" jsonschema:"title=Secret Advanced,description=Some secret advanced config with ordering." jsonschema_extras:"secret=true,order=0"`
	} `json:"advanced,omitempty" jsonschema_extras:"advanced=true"`
}

func TestGenerateSchema(t *testing.T) {
	got := GenerateSchema("Test Schema", testConfig{})
	formatted, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}
