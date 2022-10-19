package capture

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/capture/driver/image"
	"github.com/estuary/flow/go/capture/driver/ingest"
	"github.com/estuary/flow/go/flow/ops"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// NewDriver returns a new driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	endpointType pf.EndpointType,
	endpointSpec json.RawMessage,
	connectorNetwork string,
	containerName string,
	logger ops.Logger,
) (pc.DriverClient, error) {

	switch endpointType {
	case pf.EndpointType_AIRBYTE_SOURCE:
		return pc.AdaptServerToClient(image.NewDriver(containerName, connectorNetwork, logger)), nil
	case pf.EndpointType_INGEST:
		return pc.AdaptServerToClient(ingest.NewDriver(logger)), nil
	default:
		return nil, fmt.Errorf("unknown endpoint %v", endpointType)
	}
}
