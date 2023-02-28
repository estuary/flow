package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/estuary/flow/go/materialize/driver/sqlite"
	"github.com/estuary/flow/go/ops"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Driver encapsulates the various ways in which we drive connectors:
// * As a linux container.
// * As a push-based ingestion.
// * As an embedded SQLite DB (deprecated; will be removed).
//
// Depending on how a connector is configured, Driver also handles required
// "unwrapping" of the connector's endpoint configuration. For example,
// it unwraps the top-level {"image":..., "config":...} wrapper of an image
// connector configuration.
type Driver struct {
	// The following are variants of a driver's enumeration type.
	// A "remote: *grpc.ClientConn" variant may be added in the future if there's a well-defined use case.
	container *Container
	ingest    *ingestClient
	sqlite    *sqlite.InProcessServer

	// Unwrapped configuration of the endpoint.
	config json.RawMessage
}

type imageSpec struct {
	Image  string          `json:"image"`
	Config json.RawMessage `json:"config"`
}

// Validate returns an error if EndpointSpec is invalid.
func (c imageSpec) Validate() error {
	if c.Image == "" {
		return fmt.Errorf("expected `image`")
	}
	return nil
}

const runInFirecracker = false

// NewDriver returns a new Driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	endpointSpec json.RawMessage,
	endpointType pf.EndpointType,
	publisher ops.Publisher,
	network string,
) (*Driver, error) {

	if endpointType == pf.EndpointType_SQLITE {
		var srv, err = sqlite.NewInProcessServer(ctx)
		if err != nil {
			return nil, err
		}

		return &Driver{
			container: nil,
			ingest:    nil,
			sqlite:    srv,
			config:    endpointSpec,
		}, nil
	}

	// TODO(johnny): These differentiated endpoint types are inaccurate and meaningless now.
	// They both now mean simply "run a docker image".
	if endpointType == pf.EndpointType_AIRBYTE_SOURCE || endpointType == pf.EndpointType_FLOW_SINK {
		var parsedSpec = new(imageSpec)

		if err := pf.UnmarshalStrict(endpointSpec, parsedSpec); err != nil {
			return nil, fmt.Errorf("parsing connector configuration: %w", err)
		}
		var container *Container
		var err error

		if runInFirecracker {
			container, err = StartFirecracker(ctx, parsedSpec.Image, publisher)
		} else {
			container, err = StartContainer(ctx, parsedSpec.Image, network, publisher)
		}
		if err != nil {
			return nil, fmt.Errorf("starting connector container: %w", err)
		}

		return &Driver{
			container: container,
			ingest:    nil,
			sqlite:    nil,
			config:    parsedSpec.Config,
		}, nil
	}

	if endpointType == pf.EndpointType_INGEST {
		return &Driver{
			container: nil,
			ingest:    new(ingestClient),
			sqlite:    nil,
			config:    endpointSpec,
		}, nil
	}

	return nil, fmt.Errorf("unknown endpoint type %v", endpointType)
}

// MaterializeClient returns a materialization DriverClient and panics
// if this Driver's endpoint type is not suited for materialization.
func (d *Driver) MaterializeClient() pm.DriverClient {
	if d.container != nil {
		return pm.NewDriverClient(d.container.conn)
	} else if d.sqlite != nil {
		return d.sqlite.Client()
	} else {
		panic("invalid driver type for materialization")
	}
}

// CaptureClient returns a capture DriverClient and panics
// if this Driver's endpoint type is not suited for capture.
func (d *Driver) CaptureClient() pc.DriverClient {
	if d.container != nil {
		return pc.NewDriverClient(d.container.conn)
	} else if d.ingest != nil {
		return d.ingest
	} else {
		panic("invalid driver type for capture")
	}
}

// Close the Driver, returning only once its fully stopped.
func (d *Driver) Close() error {
	var err error
	if d.container != nil {
		err = d.container.Stop()
	} else if d.ingest != nil {
		err = nil // Nothing to close.
	} else if d.sqlite != nil {
		err = d.sqlite.Stop()
	}

	return err
}

// Invoke is a convenience which creates a Driver, invokes a single unary RPC
// via the provided callback, and then tears down the Driver all in one go.
func Invoke[
	Request interface {
		proto.Message
		GetEndpointSpecPtr() *json.RawMessage
		GetEndpointType() pf.EndpointType
		Validate() error
	},
	Response any,
	ResponsePtr interface {
		*Response
		proto.Message
		Validate() error
	},
](
	ctx context.Context,
	request Request,
	network string,
	publisher ops.Publisher,
	cb func(*Driver, Request) (*Response, error),
) (*Response, error) {
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("pre-flight request validation failed: %w", err)
	}

	var driver, err = NewDriver(
		ctx,
		*request.GetEndpointSpecPtr(),
		request.GetEndpointType(),
		publisher,
		network,
	)
	if err != nil {
		return nil, err
	}

	// Ensure driver is cleaned up if we fail.
	defer func() {
		if driver != nil {
			_ = driver.Close()
		}
	}()

	var response *Response
	if err = WithUnsealed(driver, request, func(request Request) error {
		response, err = cb(driver, request)
		return err
	}); err != nil {
		if status, ok := status.FromError(err); ok && status.Code() == codes.Internal {
			err = errors.New(status.Message())
		}
		return nil, fmt.Errorf("invocation failed: %w", err)
	} else if err = ResponsePtr(response).Validate(); err != nil {
		return nil, fmt.Errorf("invocation succeeded but returned invalid response: %w", err)
	}

	if err = driver.Close(); err != nil {
		return nil, fmt.Errorf("closing connector driver: %w", err)
	}
	driver = nil

	return response, nil
}
