package driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/materialize/driver/sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/grpc"
)

// NewDriver returns a new driver implementation for the given EndpointType.
func NewDriver(ctx context.Context, endpointType pf.EndpointType, config json.RawMessage) (pm.DriverClient, error) {
	switch endpointType {
	case pf.EndpointType_SQLITE:
		return adapter{sql.NewSQLiteDriver()}, nil
	case pf.EndpointType_REMOTE:
		var endpoint struct {
			Address string
		}
		if err := json.Unmarshal(config, &endpoint); err != nil {
			return nil, fmt.Errorf("parsing address: %w", err)
		}
		conn, err := grpc.DialContext(ctx, endpoint.Address)
		return pm.NewDriverClient(conn), err
	default:
		return nil, fmt.Errorf("unknown endpoint %v", endpointType)
	}
}
