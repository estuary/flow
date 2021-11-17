package image

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/flow/go/capture/driver/airbyte"
	"github.com/estuary/flow/go/flow/ops"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	protoio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

// EndpointSpec is the configuration for Flow sink connectors.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type EndpointSpec struct {
	Image  string          `json:"image"`
	Config json.RawMessage `json:"config"`
}

// Validate the configuration.
func (c EndpointSpec) Validate() error {
	if c.Image == "" {
		return fmt.Errorf("expected `image`")
	}
	return nil
}

// driver implements the pm.DriverServer interface.
// Though driver is a gRPC service stub, it's called in synchronous and
// in-process contexts to minimize ser/de & memory copies. As such it
// doesn't get to assume deep ownership of its requests, and must
// proto.Clone() shared state before mutating it.
type driver struct {
	networkName string
	logger      ops.Logger
}

// NewDriver returns a new Docker image driver.
func NewDriver(networkName string, logger ops.Logger) pm.DriverServer {
	return driver{
		networkName: networkName,
		logger:      logger,
	}
}

// Spec delegates to the `spec` command of the identified docker image.
func (d driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of proxied configuration.
	req.EndpointSpecJson = source.Config

	var resp *pm.SpecResponse
	var err = airbyte.RunConnector(ctx, source.Image, d.networkName,
		[]string{"spec"},
		nil, // No configuration is passed as files.
		func(w io.Writer) error {
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).
				WriteMsg(req)
		},
		airbyte.NewConnectorProtoOutput(
			func() proto.Message { return new(pm.SpecResponse) },
			func(m proto.Message) error {
				if resp != nil {
					return fmt.Errorf("read more than one SpecResponse")
				}
				resp = m.(*pm.SpecResponse)
				return nil
			},
		),
		d.logger,
	)
	return resp, err
}

// Validate delegates to the `validate` command of the identified docker image.
func (d driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of proxied configuration.
	req.EndpointSpecJson = source.Config

	var resp *pm.ValidateResponse
	var err = airbyte.RunConnector(ctx, source.Image, d.networkName,
		[]string{"validate"},
		nil, // No configuration is passed as files.
		func(w io.Writer) error {
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).
				WriteMsg(req)
		},
		airbyte.NewConnectorProtoOutput(
			func() proto.Message { return new(pm.ValidateResponse) },
			func(m proto.Message) error {
				if resp != nil {
					return fmt.Errorf("read more than one ValidateResponse")
				}
				resp = m.(*pm.ValidateResponse)
				return nil
			},
		),
		d.logger,
	)
	return resp, err
}

// ApplyUpsert delegates to the `apply-upsert` command of the identified docker image.
func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return d.apply(ctx, "apply-upsert", req)
}

// ApplyDelete delegates to the `apply-delete` command of the identified docker image.
func (d driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return d.apply(ctx, "apply-delete", req)
}

func (d driver) apply(ctx context.Context, variant string, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var source = new(EndpointSpec)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of proxied configuration.
	req.Materialization = proto.Clone(req.Materialization).(*pf.MaterializationSpec)
	req.Materialization.EndpointSpecJson = source.Config

	var resp *pm.ApplyResponse
	var err = airbyte.RunConnector(ctx, source.Image, d.networkName,
		[]string{variant},
		nil, // No configuration is passed as files.
		func(w io.Writer) error {
			return protoio.NewUint32DelimitedWriter(w, binary.LittleEndian).
				WriteMsg(req)
		},
		airbyte.NewConnectorProtoOutput(
			func() proto.Message { return new(pm.ApplyResponse) },
			func(m proto.Message) error {
				if resp != nil {
					return fmt.Errorf("read more than one ApplyResponse")
				}
				resp = m.(*pm.ApplyResponse)
				return nil
			},
		),
		d.logger,
	)
	return resp, err
}

// Transactions delegates to the `transactions` command of the identified docker image.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	}

	var source = new(EndpointSpec)
	if err := open.Validate(); err != nil {
		return fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, source); err != nil {
		return fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of proxied configuration.
	open.Open.Materialization = proto.Clone(open.Open.Materialization).(*pf.MaterializationSpec)
	open.Open.Materialization.EndpointSpecJson = source.Config

	return airbyte.RunConnector(
		stream.Context(),
		source.Image,
		d.networkName,
		[]string{"transactions"},
		nil, // No configuration is passed as files.
		func(w io.Writer) error { return protoWriteLoop(stream, open, w) },
		airbyte.NewConnectorProtoOutput(
			func() proto.Message { return new(pm.TransactionResponse) },
			func(m proto.Message) error { return stream.Send(m.(*pm.TransactionResponse)) },
		),
		d.logger,
	)
}

// protoWriteLoop reads |stream| and proxies messages to the container Writer.
func protoWriteLoop(
	stream pm.Driver_TransactionsServer,
	req *pm.TransactionRequest,
	w io.Writer,
) error {
	var enc = protoio.NewUint32DelimitedWriter(w, binary.LittleEndian)
	var err = enc.WriteMsg(req)

	if err != nil {
		return fmt.Errorf("proxying Open: %w", err)
	}

	for {
		if req, err = stream.Recv(); err == io.EOF {
			return nil // Clean shutdown.
		} else if err != nil {
			return fmt.Errorf("reading from runtime: %w", err)
		} else if err = enc.WriteMsg(req); err != nil {
			return fmt.Errorf("writing to connector: %w", err)
		}
	}
}

/*
// TODO(johnny): Partially-vetted code for driving a JSON version of the
// protocol, which I don't want to support quite yet but do want to keep close.

type LoadRequest struct {
	Binding int         `json:"binding"`
	Key     tuple.Tuple `json:"key"`
}

type LoadResponse struct {
	Binding  int             `json:"binding"`
	Document json.RawMessage `json:"document"`
}

type StoreRequest struct {
	Binding  int             `json:"binding"`
	Key      tuple.Tuple     `json:"key"`
	Values   tuple.Tuple     `json:"values"`
	Document json.RawMessage `json:"document"`
	Exists   bool            `json:"exists"`
}

type TxnRequest struct {
	Open    *pm.TransactionRequest_Open    `json:"open,omitempty"`
	Load    *LoadRequest                   `json:"load,omitempty"`
	Prepare *pm.TransactionRequest_Prepare `json:"prepare,omitempty"`
	Store   *StoreRequest                  `json:"store,omitempty"`
	Commit  *pm.TransactionRequest_Commit  `json:"commit,omitempty"`
}

type TxnResponse struct {
	Opened    *pm.TransactionResponse_Opened    `json:"opened,omitempty"`
	Loaded    *LoadResponse                     `json:"loaded,omitempty"`
	Prepared  *pm.TransactionResponse_Prepared  `json:"prepared,omitempty"`
	Committed *pm.TransactionResponse_Committed `json:"committed,omitempty"`
}


// jsonWriteLoop reads |stream| and proxies JSON messages to the container Writer.
func jsonWriteLoop(
	stream pm.Driver_TransactionsServer,
	open *pm.TransactionRequest,
	w io.Writer,
) error {

	var enc = json.NewEncoder(w)
	if err := enc.Encode(TxnRequest{Open: open.Open}); err != nil {
		return fmt.Errorf("proxying Open: %w", err)
	}

	for round := 0; true; round++ {
		var loadIt = pm.NewLoadIterator(stream)

		for loadIt.Next() {
			if err := enc.Encode(TxnRequest{
				Load: &LoadRequest{
					Binding: loadIt.Binding,
					Key:     loadIt.Key,
				},
			}); err != nil {
				return fmt.Errorf("encoding Load: %w", err)
			}
		}
		if loadIt.Err() == io.EOF {
			return nil // Clean shutdown.
		} else if loadIt.Err() != nil {
			return loadIt.Err()
		}

		if err := enc.Encode(TxnRequest{
			Prepare: loadIt.Prepare(),
		}); err != nil {
			return fmt.Errorf("encoding Prepare: %w", err)
		}

		var storeIt = pm.NewStoreIterator(stream)
		for storeIt.Next() {
			if err := enc.Encode(TxnRequest{
				Store: &StoreRequest{
					Binding:  storeIt.Binding,
					Key:      storeIt.Key,
					Values:   storeIt.Values,
					Document: storeIt.RawJSON,
					Exists:   storeIt.Exists,
				},
			}); err != nil {
				return fmt.Errorf("encoding Load: %w", err)
			}
		}
		if storeIt.Err() != nil {
			return storeIt.Err()
		}

		if err := enc.Encode(TxnRequest{
			Commit: storeIt.Commit(),
		}); err != nil {
			return fmt.Errorf("encoding Commit: %w", err)
		}
	}

	return nil
}

func jsonResponseHandler(
	stream pm.Driver_TransactionsServer,
	resp *pm.TransactionResponse,
	i interface{},
) error {
	var r = i.(*TxnResponse)

	if r.Opened != nil {
		return pm.WriteOpened(
			stream,
			&resp,
			r.Opened,
		)
	} else if r.Loaded != nil {
		return pm.StageLoaded(stream, &resp, r.Loaded.Binding, r.Loaded.Document)
	} else if r.Prepared != nil {
		return pm.WritePrepared(stream, &resp, r.Prepared)
	} else if r.Committed != nil {
		return pm.WriteCommitted(stream, &resp)
	} else {
		return fmt.Errorf("unexpected connector output record: %#v", r)
	}
}
*/
