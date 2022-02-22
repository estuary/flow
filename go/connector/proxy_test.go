package connector

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/stretchr/testify/require"
)

func TestRustBinDir(t *testing.T) {
	var proxy, _ = NewProxy("test_image", ProxyFlowCapture, FlowCapture, nil)
	defer proxy.Cleanup()
	os.Setenv("FLOW_BINARY_DIR", "/test_rustbin")
	require.Equal(t, proxy.getRustBinDir(), "/test_rustbin")
	os.Unsetenv("FLOW_BINARY_DIR")
	require.Equal(t, proxy.getRustBinDir(), "/usr/local/bin")

}

func TestPrepareFlowConnectorProxyBinary(t *testing.T) {
	var tmpRustBinDir, err = ioutil.TempDir("", "test-rustbin")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(tmpRustBinDir, "flow-connector-proxy"), []byte("random content"), 0777)
	require.NoError(t, err)
	defer os.Remove(tmpRustBinDir)

	var proxy, _ = NewProxy("test_image", ProxyFlowCapture, FlowCapture, nil)
	defer proxy.Cleanup()
	os.Setenv("FLOW_BINARY_DIR", tmpRustBinDir)

	var connectorProxyPath string
	connectorProxyPath, err = proxy.prepareFlowConnectorProxyBinary()
	require.NoError(t, err)
	bytes, err := os.ReadFile(connectorProxyPath)
	require.NoError(t, err)
	require.Equal(t, "random content", string(bytes))
}

func TestGetConnectorProtocol(t *testing.T) {
	var logger = ops.StdLogger()
	var proxy, _ = NewProxy("test_image", ProxyFlowCapture, FlowCapture, logger)
	defer proxy.Cleanup()

	var testConfig = container.Config{}
	require.Equal(t, proxy.getConnectorProtocol(&testConfig), "flow-capture")
	testConfig.Labels = map[string]string{"CONNECTOR_PROTOCOL": "test-flow-capture"}
	require.Equal(t, proxy.getConnectorProtocol(&testConfig), "test-flow-capture")
}
