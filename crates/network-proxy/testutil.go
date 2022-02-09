package networkproxy

import (
	"encoding/base64"
	"os"

	sf "github.com/estuary/connectors/network-proxy-service/sshforwarding"
)

// Configuration set based on sshforwarding/test_sshd_configs/docker-compose.yaml.
func CreateSshForwardingTestConfig(keyFilePath string, remotePort uint16) (*NetworkProxyConfig, error) {
	var b, err = os.ReadFile(keyFilePath)
	if err != nil {
		return nil, err
	}
	return &NetworkProxyConfig{
		ProxyType: "sshForwarding",
		SshForwardingConfig: sf.SshForwardingConfig{
			SshEndpoint:         "ssh://127.0.0.1:2222",
			SshPrivateKeyBase64: base64.RawStdEncoding.EncodeToString(b),
			SshUser:             "flowssh",
			RemoteHost:          "127.0.0.1",
			RemotePort:          remotePort,
			LocalPort:           12345,
		},
	}, nil
}
