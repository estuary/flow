package connector

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRustBinDir(t *testing.T) {
	os.Setenv("FLOW_BINARY_DIR", "/test_rustbin")
	require.Equal(t, getRustBinDir(), "/test_rustbin")
	os.Unsetenv("FLOW_BINARY_DIR")
	require.Equal(t, getRustBinDir(), "/usr/local/bin")

}

func TestPrepareFlowConnectorProxyBinary(t *testing.T) {
	var tmpRustBinDir, err = ioutil.TempDir("", "test-rustbin")
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(tmpRustBinDir, "flow-connector-proxy"), []byte("random content"), 0777)
	require.NoError(t, err)
	defer os.Remove(tmpRustBinDir)

	var proxy, _ = NewProxy(nil)
	defer proxy.Cleanup()
	os.Setenv("FLOW_BINARY_DIR", tmpRustBinDir)

	var connectorProxyPath string
	connectorProxyPath, err = proxy.copyFlowConnectorProxyBinary()
	require.NoError(t, err)
	bytes, err := os.ReadFile(connectorProxyPath)
	require.NoError(t, err)
	require.Equal(t, "random content", string(bytes))
}
