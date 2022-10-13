package image

import (
	"context"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow/ops"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/gogo/protobuf/proto"
)

// driver implements the pm.DriverServer interface.
type driver struct {
	containerName string
	networkName   string
	logger        ops.Logger
}

// NewDriver returns a new container image DriverServer.
func NewDriver(containerName, networkName string, logger ops.Logger) pc.DriverServer {
	return driver{
		containerName: containerName,
		networkName:   networkName,
		logger:        logger,
	}
}

// Spec delegates to `spec` of the connector image.
func (d driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var resp = new(pc.SpecResponse)
	var err = connector.UnaryRPC(ctx, "spec", connector.Capture, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// Discover delegates to `discover` of the connector image.
func (d driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var resp = new(pc.DiscoverResponse)
	var err = connector.UnaryRPC(ctx, "discover", connector.Capture, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// Validate delegates to `validate` of the connector image.
func (d driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var resp = new(pc.ValidateResponse)
	var err = connector.UnaryRPC(ctx, "validate", connector.Capture, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// ApplyUpsert delegates to `apply` of the connector image.
func (d driver) ApplyUpsert(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	var resp = new(pc.ApplyResponse)
	var err = connector.UnaryRPC(ctx, "apply-upsert", connector.Capture, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// ApplyDelete delegates to `apply-delete` of the connector image.
func (d driver) ApplyDelete(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	var resp = new(pc.ApplyResponse)
	var err = connector.UnaryRPC(ctx, "apply-delete", connector.Capture, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// Pull delegates to `read` of the connector image.
func (d driver) Pull(stream pc.Driver_PullServer) error {
	return connector.StreamRPC(
		stream,
		"pull",
		connector.Capture,
		func() proto.Message { return new(pc.PullRequest) },
		func() proto.Message { return new(pc.PullResponse) },
		d.logger,
		d.networkName,
		d.containerName,
	)
}
