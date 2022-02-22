package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/estuary/flow/go/flow/ops"
	"github.com/sirupsen/logrus"
)

// Constants related to the flow-connector-proxy.
// If envvar FLOW_RUST_BIN is set, it is "${FLOW_RUST_BIN}/flow-connector-proxy". Otherwise
// use the default of "/usr/local/bin/flow-connector-proxy".
const flowConnectorProxy = "flow-connector-proxy"
const defaultFlowRustBinDir = "/usr/local/bin"
const flowBinaryDirEnvKey = "FLOW_BINARY_DIR"

// The label in the connector image to provide protocol of the connector optionally.
const connectorProtocolLabelKey = "CONNECTOR_PROTOCOL"

type ProxyCommand string

const (
	ProxyFlowCapture     ProxyCommand = "proxy-flow-capture"
	ProxyFlowMaterialize ProxyCommand = "proxy-flow-materialize"
)

type ConnectorProtocol string

const (
	Airbyte         ConnectorProtocol = "airbyte"
	FlowCapture     ConnectorProtocol = "flow-capture"
	FlowMaterialize ConnectorProtocol = "flow-materialize"
)

type Proxy struct {
	// Temp dir for hosting binaries to be mounted to the proxied image.
	tmpDir string
	// The connector image being proxied.
	image string
	// The proxy command to execute.
	proxyCommand ProxyCommand
	// The default protocol that the connector image uses. It could be overridden
	// by a label with key "CONNECTOR_PROTOCOL" from the image.
	defaultConnectorProtocol ConnectorProtocol
	// For recording log messages.
	logger ops.Logger
}

// NewProxy creates an proxy object for managing a connector image.
func NewProxy(image string, proxyCommand ProxyCommand, defaultConnectorProtocol ConnectorProtocol, logger ops.Logger) (*Proxy, error) {
	if tmpDir, err := ioutil.TempDir("", "flow-execution"); err != nil {
		return nil, fmt.Errorf("create tempdir: %w", err)
	} else {
		return &Proxy{tmpDir, image, proxyCommand, defaultConnectorProtocol, logger}, nil
	}
}

func (p *Proxy) PrepareToRun(
	ctx context.Context,
	operation string,
	commandBuilder *DockerRunCommandBuilder) error {

	var containerConfig *container.Config
	var err error

	if containerConfig, err = p.pullAndContainerConfig(ctx, p.image, p.logger); err != nil {
		return fmt.Errorf("prepare image: %w", err)
	}

	var connectorProxyPath string
	if connectorProxyPath, err = p.prepareFlowConnectorProxyBinary(); err != nil {
		return fmt.Errorf("prepare flow connector proxy binary: %w", err)
	}

	var connectorProtocol = p.getConnectorProtocol(containerConfig)

	commandBuilder.SetEntrypoint(
		connectorProxyPath,
	).AddMount(
		connectorProxyPath, connectorProxyPath,
	)

	for _, entrypoint := range containerConfig.Entrypoint {
		commandBuilder.AddArgs([]string{"--connector-entrypoint", entrypoint})
	}

	commandBuilder.AddArgs([]string{
		string(p.proxyCommand),
		connectorProtocol,
		operation,
	})
	return nil
}

func (p *Proxy) Cleanup() {
	os.RemoveAll(p.tmpDir)
}

// pulls down the image, and returns its labels.
func (p *Proxy) pullAndContainerConfig(ctx context.Context, image string, logger ops.Logger) (*container.Config, error) {
	var output bytes.Buffer

	if cli, err := client.NewClientWithOpts(client.FromEnv); err != nil {
		return nil, fmt.Errorf("creating docker client: %w", err)
	} else if reader, err := cli.ImagePull(ctx, image, types.ImagePullOptions{}); err != nil {
		return nil, fmt.Errorf("start pulling docker image: %w", err)
	} else if _, err := io.Copy(&output, reader); err != nil {
		logger.Log(logrus.ErrorLevel, logrus.Fields{"error": err, "docker output": output.String()},
			"failed to send connector input")
		return nil, fmt.Errorf("get image pull output: %w", err)
	} else if imageInfo, _, err := cli.ImageInspectWithRaw(ctx, image); err != nil {
		return nil, fmt.Errorf("inspect image: %w", err)
	} else {
		return imageInfo.Config, nil
	}
}

func (p *Proxy) prepareFlowConnectorProxyBinary() (string, error) {
	var connectorProxyPath = filepath.Join(p.tmpDir, "connector_proxy")
	if input, err := ioutil.ReadFile(filepath.Join(p.getRustBinDir(), flowConnectorProxy)); err != nil {
		return "", fmt.Errorf("read connector proxy binary from source: %w", err)
	} else if err = ioutil.WriteFile(connectorProxyPath, input, 0751); err != nil {
		return "", fmt.Errorf("write connector proxy binary: %w", err)
	}

	return connectorProxyPath, nil
}

func (p *Proxy) getRustBinDir() string {
	if rustBinDir, ok := os.LookupEnv(flowBinaryDirEnvKey); ok {
		return rustBinDir
	}

	return defaultFlowRustBinDir
}

func (p *Proxy) getConnectorProtocol(containerConfig *container.Config) string {
	var connectorProtocol, ok = containerConfig.Labels[connectorProtocolLabelKey]
	if !ok {
		p.logger.Log(logrus.WarnLevel, nil,
			fmt.Sprintf("Label %s not found in the image, using default protocol.", connectorProtocolLabelKey),
		)
		return string(p.defaultConnectorProtocol)
	}
	return connectorProtocol
}
