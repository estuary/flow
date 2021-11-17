package capture

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/capture/driver/airbyte"
	"github.com/estuary/flow/go/flow/ops"
	pc "github.com/estuary/protocols/capture"
	pf "github.com/estuary/protocols/flow"
)

// NewDriver returns a new driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	endpointType pf.EndpointType,
	endpointSpec json.RawMessage,
	connectorNetwork string,
	logger ops.Logger,
) (pc.DriverClient, error) {

	switch endpointType {
	case pf.EndpointType_AIRBYTE_SOURCE:
		return pc.AdaptServerToClient(airbyte.NewDriver(connectorNetwork, logger)), nil
	default:
		return nil, fmt.Errorf("unknown endpoint %v", endpointType)
	}
}
