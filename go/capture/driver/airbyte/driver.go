package airbyte

import (
	"context"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
)

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
	return &pc.ValidateResponse{
		ResourcePath: []string{"placeholder"},
	}, nil
}

func (driver) Capture(*pc.CaptureRequest, pc.Driver_CaptureServer) error {
	return fmt.Errorf("not implemented")
}
