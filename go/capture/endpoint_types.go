package capture

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/capture/driver/airbyte"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

// NewDriver returns a new driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	endpointType pf.EndpointType,
	endpointSpec json.RawMessage,
	tempdir string,
) (pc.DriverClient, error) {

	switch endpointType {
	case pf.EndpointType_AIRBYTE_SOURCE:
		return AdaptServerToClient(airbyte.NewDriver()), nil
	case pf.EndpointType_REMOTE:
		var cfg struct {
			Endpoint protocol.Endpoint
		}
		if err := json.Unmarshal(endpointSpec, &cfg); err != nil {
			return nil, fmt.Errorf("parsing config: %w", err)
		} else if err = cfg.Endpoint.Validate(); err != nil {
			return nil, err
		}
		conn, err := grpc.DialContext(ctx, string(cfg.Endpoint))
		return pc.NewDriverClient(conn), err
	default:
		return nil, fmt.Errorf("unknown endpoint %v", endpointType)
	}
}
