package connector

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/estuary/flow/go/flow/ops"
	"github.com/sirupsen/logrus"
)

// Constants related to the path of flow-connector-proxy.
// If envvar FLOW_RUST_BIN is set, it is "${FLOW_RUST_BIN}/flow-connector-proxy". Otherwise,
// use the default of "/usr/local/bin/flow-connector-proxy".
const flowConnectorProxy = "flow-connector-proxy"
const defaultFlowRustBinDir = "/usr/local/bin"
const flowBinaryDirEnvKey = "FLOW_BINARY_DIR"

// Corresponding to the ProxyCommand specified in crates/connector_proxy/src/main.rs
type ProxyCommand string

const (
	ProxyFlowCapture     ProxyCommand = "proxy-flow-capture"
	ProxyFlowMaterialize ProxyCommand = "proxy-flow-materialize"
)

// The Proxy is responsible for the preparation of starting a connector docker image with the flow-connector-proxy,
type Proxy struct {
	// Temp dir for hosting binaries (e.g. flow-connector-proxy) and related files to be mounted to the proxied image.
	tmpDir string
	// For recording log messages.
	logger ops.Logger
}

func NewProxy(logger ops.Logger) (*Proxy, error) {
	if tmpDir, err := ioutil.TempDir("", "flow-execution"); err != nil {
		return nil, fmt.Errorf("create tempdir: %w", err)
	} else {
		return &Proxy{tmpDir, logger}, nil
	}
}

func (p *Proxy) PrepareToRun(ctx context.Context, image string, proxyCommand ProxyCommand, operation string) (imageArgs []string, args []string, e error) {
	var err error
	var connectorProxyPath string
	if connectorProxyPath, err = p.copyFlowConnectorProxyBinary(); err != nil {

		return nil, nil, fmt.Errorf("prepare flow connector proxy binary: %w", err)
	}

	var inspectOutputPath string
	if err = p.pullImage(ctx, image); err != nil {
		return nil, nil, fmt.Errorf("pull image: %w", err)
	} else if inspectOutputPath, err = p.inspectImage(ctx, image); err != nil {
		return nil, nil, fmt.Errorf("inspect image: %w", err)
	}

	imageArgs = []string{
		"--entrypoint", connectorProxyPath,
		"--mount", fmt.Sprintf("type=bind,source=%[1]s,target=%[1]s", inspectOutputPath),
		"--mount", fmt.Sprintf("type=bind,source=%[1]s,target=%[1]s", connectorProxyPath),
	}

	args = []string{
		fmt.Sprintf("--image-inspect-json-path=%s", inspectOutputPath),
		string(proxyCommand),
		operation,
	}

	return imageArgs, args, nil
}

func (p *Proxy) Cleanup() {
	os.RemoveAll(p.tmpDir)
}

func (p *Proxy) pullImage(ctx context.Context, image string) error {
	var combinedOutput, err = exec.CommandContext(ctx, "docker", "pull", image).CombinedOutput()
	p.logger.Log(logrus.InfoLevel, nil, fmt.Sprintf("output from docker pull: %s", combinedOutput))
	if err != nil {
		return fmt.Errorf("pull image: %w", err)
	}
	return nil
}

func (p *Proxy) inspectImage(ctx context.Context, image string) (string, error) {
	var inspectOutputPath = filepath.Join(p.tmpDir, "image_inspect.json")
	if output, err := exec.CommandContext(ctx, "docker", "inspect", image).Output(); err != nil {
		return "", fmt.Errorf("inspect image: %w", err)
	} else if err = ioutil.WriteFile(inspectOutputPath, output, 0666); err != nil {
		return "", fmt.Errorf("write connector proxy binary: %w", err)
	} else {
		return inspectOutputPath, nil
	}
}

func (p *Proxy) copyFlowConnectorProxyBinary() (string, error) {
	var connectorProxyPath = filepath.Join(p.tmpDir, "connector_proxy")
	if input, err := ioutil.ReadFile(filepath.Join(getRustBinDir(), flowConnectorProxy)); err != nil {
		return "", fmt.Errorf("read connector proxy binary from source: %w", err)
	} else if err = ioutil.WriteFile(connectorProxyPath, input, 0751); err != nil {
		return "", fmt.Errorf("write connector proxy binary: %w", err)
	}

	return connectorProxyPath, nil
}

func getRustBinDir() string {
	if rustBinDir, ok := os.LookupEnv(flowBinaryDirEnvKey); ok {
		return rustBinDir
	}

	return defaultFlowRustBinDir
}
