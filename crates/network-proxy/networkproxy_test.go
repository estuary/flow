package networkproxy

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

const TestRsaFilePath = "sshforwarding/test_sshd_configs/keys/id_rsa"

func TestNetworkProxyConfig_Validate(t *testing.T) {
	var nilConfig *NetworkProxyConfig
	require.NoError(t, nilConfig.Validate())

	var unsupportedConfig = &NetworkProxyConfig{ProxyType: "unsupported"}
	require.Error(t, unsupportedConfig.Validate(), "expected validation error for unsupported proxy type.")

	var typeOnlyProxyConfig = NetworkProxyConfig{ProxyType: "sshForwarding"}
	require.Error(t, typeOnlyProxyConfig.Validate(), "expected validation error for ssh_forwording config without real configs.")

	var sshForwardingConfig, err = CreateSshForwardingTestConfig(TestRsaFilePath, 15432)
	require.NoError(t, err)
	require.NoError(t, sshForwardingConfig.Validate())
}

func TestSshForwardConfig_startSuccessfully(t *testing.T) {
	// remotePort set to be 2222. Tunnel to itself for testing.
	var config, err = CreateSshForwardingTestConfig(TestRsaFilePath, 2222)
	require.NoError(t, err)
	require.NoError(t, config.Start())
}

func TestSshForwardConfig_startWithDefaultWithBadSshEndpoint(t *testing.T) {
	var config, err = CreateSshForwardingTestConfig(TestRsaFilePath, 2222)
	require.NoError(t, err)
	config.SshForwardingConfig.SshEndpoint = "bad_endpoint"
	var stubStderr bytes.Buffer
	err = config.startInternal(1, &stubStderr)
	require.Contains(t, stubStderr.String(), "ssh_endpoint parse error")
}
