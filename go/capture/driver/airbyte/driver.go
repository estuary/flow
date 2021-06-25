package airbyte

import (
	"context"
	"encoding/json"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// sourceConfig is the configuration for Airbyte source connectors.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type sourceConfig struct {
	Image  string
	Config json.RawMessage
}

// Validate the configuration.
func (c sourceConfig) Validate() error {
	if c.Image == "" {
		return fmt.Errorf("expected `image`")
	}
	return nil
}

// streamConfig is the configuration for Airbyte source streams.
type streamConfig struct {
	Stream    string
	Namespace string
}

// Validate the configuration.
func (c streamConfig) Validate() error {
	if c.Stream == "" {
		return fmt.Errorf("expected `stream`")
	}
	// Namespace is optional.
	return nil
}

// driver implements the pm.DriverServer interface.
type driver struct{}

// NewDriver returns a new JSON docker image driver.
func NewDriver() pc.DriverServer { return driver{} }

func (driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var source = new(sourceConfig)
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var resp = new(pc.ValidateResponse)
	for _, binding := range req.Bindings {
		var stream = new(streamConfig)
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, stream); err != nil {
			return nil, fmt.Errorf("parsing stream configuration: %w", err)
		}
		resp.Bindings = append(resp.Bindings, &pc.ValidateResponse_Binding{
			ResourcePath: []string{stream.Stream},
		})
	}
	return resp, nil
}

func (driver) Capture(*pc.CaptureRequest, pc.Driver_CaptureServer) error {
	return fmt.Errorf("not implemented")
}
