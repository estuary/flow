package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
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

// NewDriver returns a new Driver implementation for the given EndpointType.
func NewDriver(
	ctx context.Context,
	config json.RawMessage,
	connectorType string,
	publisher ops.Publisher,
	network string,
	exposePorts ExposePorts,
) (*Driver, error) {

	if connectorType == "IMAGE" {
		var parsedSpec = new(imageSpec)

		if err := pf.UnmarshalStrict(config, parsedSpec); err != nil {
			return nil, fmt.Errorf("parsing connector configuration: %w", err)
		}
		container, err := StartContainer(ctx, parsedSpec.Image, network, publisher, exposePorts)
		if err != nil {
			return nil, fmt.Errorf("starting connector container: %w", err)
		}

		return &Driver{
			container: container,
			config:    parsedSpec.Config,
		}, nil
	}

	return nil, fmt.Errorf("unknown connector type %v", connectorType)
}

// MaterializeClient returns a materialization DriverClient and panics
// if this Driver's endpoint type is not suited for materialization.
func (d *Driver) MaterializeClient() pm.ConnectorClient {
	if d.container != nil {
		return pm.NewConnectorClient(d.container.conn)
	} else {
		panic("invalid driver type for materialization")
	}
}

// CaptureClient returns a capture DriverClient and panics
// if this Driver's endpoint type is not suited for capture.
func (d *Driver) CaptureClient() pc.ConnectorClient {
	if d.container != nil {
		return pc.NewConnectorClient(d.container.conn)
	} else {
		panic("invalid driver type for capture")
	}
}

func (d *Driver) GetContainerClientConn() *grpc.ClientConn {
	if d.container != nil {
		return d.container.conn
	} else {
		panic("invalid driver type for GetContainerClientConn")
	}
}

// Close the Driver, returning only once its fully stopped.
func (d *Driver) Close() error {
	var err error
	if d.container != nil {
		err = d.container.Stop()
	}

	return err
}

// Invoke is a convenience which creates a Driver, invokes a single unary RPC
// via the provided callback, and then tears down the Driver all in one go.
func Invoke[
	Response any,
	Request interface {
		proto.Message
		InvokeConfig() (*json.RawMessage, string)
		Validate_() error
	},
	ResponsePtr interface {
		*Response
		proto.Message
		Validate() error
	},
	Stream interface {
		Recv() (*Response, error)
		Send(Request) error
		CloseSend() error
	},
](
	ctx context.Context,
	request Request,
	network string,
	publisher ops.Publisher,
	cb func(*Driver) (Stream, error),
) (*Response, error) {
	if err := request.Validate_(); err != nil {
		return nil, fmt.Errorf("pre-flight request validation failed: %w", err)
	}

	var configPtr, connectorType = request.InvokeConfig()

	var driver, err = NewDriver(
		ctx,
		*configPtr,
		connectorType,
		publisher,
		network,
		nil,
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
		var stream, err = cb(driver)
		if err != nil {
			return err
		} else if err = stream.Send(request); err != nil {
			_, err = stream.Recv()
			return err
		} else if response, err = stream.Recv(); err != nil {
			return err
		} else if stream.CloseSend(); err != nil {
			return fmt.Errorf("sending CloseSend: %w", err)
		} else if _, err = stream.Recv(); err != io.EOF {
			return fmt.Errorf("expected EOF but received: %w", err)
		}
		return nil
	}); err != nil {
		return nil, pf.UnwrapGRPCError(err)
	} else if err = ResponsePtr(response).Validate(); err != nil {
		return nil, fmt.Errorf("invocation succeeded but returned invalid response: %w", err)
	}

	if err = driver.Close(); err != nil {
		return nil, fmt.Errorf("closing connector driver: %w", err)
	}
	driver = nil

	return response, nil
}
