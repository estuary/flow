package connector

import (
	"context"
	"encoding/json"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	"google.golang.org/grpc"
)

// ingestEndpointSpec is the configuration for Ingest captures.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type ingestEndpointSpec struct{}

// Validate the configuration.
func (c ingestEndpointSpec) Validate() error {
	return nil
}

// ingestResourceSpec is the configuration for bound ingestion resources.
type ingestResourceSpec struct {
	// TODO(johnny): I'm not at all sure that "name" is what we want,
	// but we require *something* to produce distinct resource paths,
	// and ingest captures *should* have some means of naming bindings
	// that's decoupled from the bound collection name.
	Name string `json:"name"`
}

// Validate the configuration.
func (c ingestResourceSpec) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("missing name")
	}
	return nil
}

// ingestClient implements the pc.DriverClient interface.
// Though driver is a gRPC service stub, it's called in synchronous and
// in-process contexts to minimize ser/de & memory copies. As such it
// doesn't get to assume deep ownership of its requests, and must
// proto.Clone() shared state before mutating it.
type ingestClient struct{}

var _ pc.DriverClient = new(ingestClient)

// Spec returns the specification of the ingest driver.
func (d *ingestClient) Spec(ctx context.Context, req *pc.SpecRequest, _ ...grpc.CallOption) (*pc.SpecResponse, error) {
	var source = new(ingestEndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var reflector = jsonschema.Reflector{ExpandedStruct: true}

	endpointSchema, err := reflector.Reflect(new(ingestEndpointSpec)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := reflector.Reflect(new(ingestResourceSpec)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev",
	}, nil
}

// Discover is a no-op.
func (d *ingestClient) Discover(ctx context.Context, req *pc.DiscoverRequest, _ ...grpc.CallOption) (*pc.DiscoverResponse, error) {
	var source = new(ingestEndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	var resp = new(pc.DiscoverResponse)
	return resp, nil
}

// Validate is a no-op.
func (d *ingestClient) Validate(ctx context.Context, req *pc.ValidateRequest, _ ...grpc.CallOption) (*pc.ValidateResponse, error) {
	var source = new(ingestEndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}

	// Parse stream bindings and send back their resource paths.
	var resp = new(pc.ValidateResponse)
	for _, binding := range req.Bindings {
		var resource = new(ingestResourceSpec)
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, resource); err != nil {
			return nil, fmt.Errorf("parsing resource configuration: %w", err)
		}
		resp.Bindings = append(resp.Bindings, &pc.ValidateResponse_Binding{
			ResourcePath: []string{resource.Name},
		})
	}
	return resp, nil
}

// ApplyUpsert is a no-op.
func (d *ingestClient) ApplyUpsert(context.Context, *pc.ApplyRequest, ...grpc.CallOption) (*pc.ApplyResponse, error) {
	return new(pc.ApplyResponse), nil
}

// ApplyDelete is a no-op.
func (d *ingestClient) ApplyDelete(context.Context, *pc.ApplyRequest, ...grpc.CallOption) (*pc.ApplyResponse, error) {
	return new(pc.ApplyResponse), nil
}

// Pull is not implemented.
func (d *ingestClient) Pull(context.Context, ...grpc.CallOption) (pc.Driver_PullClient, error) {
	return nil, fmt.Errorf("ingest client doesn't support Pull (should be using Push instead)")
}
