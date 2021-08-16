package capture

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/capture/driver/airbyte"
	pc "github.com/estuary/protocols/capture"
	pf "github.com/estuary/protocols/flow"
	"go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

// NewDriver returns a new driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	endpointType pf.EndpointType,
	endpointSpec json.RawMessage,
	tempdir string,
	connectorNetwork string,
) (pc.DriverClient, error) {

	switch endpointType {
	case pf.EndpointType_AIRBYTE_SOURCE:
		return AdaptServerToClient(airbyte.NewDriver(connectorNetwork)), nil
	case pf.EndpointType_REMOTE:
		var cfg struct {
			Address protocol.Endpoint
		}
		if err := json.Unmarshal(endpointSpec, &cfg); err != nil {
			return nil, fmt.Errorf("parsing config: %w", err)
		} else if err = cfg.Address.Validate(); err != nil {
			return nil, err
		}
		conn, err := grpc.DialContext(ctx, string(cfg.Address))
		return pc.NewDriverClient(conn), err
	default:
		return nil, fmt.Errorf("unknown endpoint %v", endpointType)
	}
}
