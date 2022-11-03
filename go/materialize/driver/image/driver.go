package image

import (
	"context"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow/ops"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/proto"
)

// driver implements the pm.DriverServer interface.
type driver struct {
	containerName string
	networkName   string
	logger        ops.Logger
}

// NewDriver returns a new container image DriverServer.
func NewDriver(containerName, networkName string, logger ops.Logger) pm.DriverServer {
	return driver{
		containerName: containerName,
		networkName:   networkName,
		logger:        logger,
	}
}

// Spec delegates to `spec` of the connector image.
func (d driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	var resp = new(pm.SpecResponse)
	var err = connector.UnaryRPC(ctx, "spec", connector.Materialize, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// Validate delegates to `validate` of the connector image.
func (d driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var resp = new(pm.ValidateResponse)
	var err = connector.UnaryRPC(ctx, "validate", connector.Materialize, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// ApplyUpsert delegates to `apply-upsert` of the connector image.
func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var resp = new(pm.ApplyResponse)
	var err = connector.UnaryRPC(ctx, "apply-upsert", connector.Materialize, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// ApplyDelete delegates to `apply-delete` of the connector image.
func (d driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var resp = new(pm.ApplyResponse)
	var err = connector.UnaryRPC(ctx, "apply-delete", connector.Materialize, req, resp, d.logger, d.networkName, d.containerName)
	return resp, err
}

// Transactions delegates to `transactions` of the connector image.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	return connector.StreamRPC(
		stream,
		"transactions",
		connector.Materialize,
		func() proto.Message { return new(pm.TransactionRequest) },
		func() proto.Message { return new(pm.TransactionResponse) },
		d.logger,
		d.networkName,
		d.containerName,
	)
}
