package sshforwarding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSshForwardConfig_Validate(t *testing.T) {
	var validConfig = SshForwardingConfig{
		SshEndpoint:         "test_endpoint",
		SshPrivateKeyBase64: "test_private_key",
		SshUser:             "test_ssh_user",
		RemoteHost:          "remote_host",
		RemotePort:          1234,
	}

	require.NoError(t, validConfig.Validate())

	var MissingSshEndpoint = validConfig
	MissingSshEndpoint.SshEndpoint = ""
	require.Error(t, MissingSshEndpoint.Validate(), "expected validation error if ssh_endpoint is missing")

	var MissingRemoteHost = validConfig
	MissingRemoteHost.RemoteHost = ""
	require.Error(t, MissingRemoteHost.Validate(), "expected validation error if remote_host is missing")

	var MissingSshPrivateKey = validConfig
	MissingSshPrivateKey.SshPrivateKeyBase64 = ""
	require.Error(t, MissingSshPrivateKey.Validate(), "expected validation error if ssh_private_key_base64 is missing")
}
