package driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/materialize/driver/jsonimage"
	"github.com/estuary/flow/go/materialize/driver/postgres"
	"github.com/estuary/flow/go/materialize/driver/snowflake"
	"github.com/estuary/flow/go/materialize/driver/sqlite"
	"github.com/estuary/flow/go/materialize/driver/webhook"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

// NewDriver returns a new driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	endpointType pf.EndpointType,
	endpointSpec json.RawMessage,
	tempdir string,
) (pm.DriverClient, error) {

	switch endpointType {
	case pf.EndpointType_SQLITE:
		return AdaptServerToClient(sqlite.NewSQLiteDriver()), nil
	case pf.EndpointType_POSTGRESQL:
		return AdaptServerToClient(postgres.NewPostgresDriver()), nil
	case pf.EndpointType_SNOWFLAKE:
		return AdaptServerToClient(snowflake.NewDriver(tempdir)), nil
	case pf.EndpointType_WEBHOOK:
		return AdaptServerToClient(webhook.NewDriver()), nil
	case pf.EndpointType_FLOW_SINK:
		return AdaptServerToClient(jsonimage.NewDriver()), nil
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
		return pm.NewDriverClient(conn), err
	default:
		return nil, fmt.Errorf("unknown endpoint %v", endpointType)
	}
}
