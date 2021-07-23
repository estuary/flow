package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/estuary/flow/go/materialize/lifecycle"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

// NewDriver returns a new Webhook driver.
func NewDriver() pm.DriverServer { return driver{} }

type config struct {
	Address pb.Endpoint
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	return c.Address.Validate()
}

type resource struct {
	// Path which is joined with the base Address to build a complete URL.
	RelativePath string
}

func (r resource) Validate() error {
	if _, err := url.Parse(r.RelativePath); err != nil {
		return fmt.Errorf("relativePath: %w", err)
	}
	return nil
}

func (r resource) URL() *url.URL {
	var u, err = url.Parse(r.RelativePath)
	if err != nil {
		panic(err)
	}
	return u
}

// Validate validates the Webhook configuration and constrains projections
// to the document root (only).
func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {

		// Verify that the resource parses, and joins into an absolute URL.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var resolved = cfg.Address.URL().ResolveReference(res.URL())
		if !resolved.IsAbs() {
			return nil, fmt.Errorf("resolved webhook address %s is not absolute", resolved)
		}

		var constraints = make(map[string]*pm.Constraint)
		for _, projection := range binding.Collection.Projections {
			var constraint = new(pm.Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			default:
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Webhooks only materialize the full document"
			}
			constraints[projection.Field] = constraint
		}

		out = append(out, &pm.ValidateResponse_Binding{
			Constraints: constraints,
			// Only delta updates are supported by webhooks.
			DeltaUpdates: true,
			ResourcePath: []string{resolved.String()},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

// Apply is a no-op.
func (driver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

// Transactions implements the DriverServer interface.
func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var log = log.WithField("address", cfg.Address)
	var addresses []*url.URL

	for _, binding := range open.Open.Materialization.Bindings {
		// Join paths of each binding with the base URL.
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		addresses = append(addresses, cfg.Address.URL().ResolveReference(res.URL()))
	}

	var transactor = &transactor{
		ctx:       stream.Context(),
		addresses: addresses,
		bodies:    make([]bytes.Buffer, len(open.Open.Materialization.Bindings)),
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: nil},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	return lifecycle.RunTransactions(stream, transactor, log)
}

type transactor struct {
	ctx       context.Context
	addresses []*url.URL
	bodies    []bytes.Buffer
}

// Load should not be called and panics.
func (d *transactor) Load(_ *lifecycle.LoadIterator, _ <-chan struct{}, _ func(int, json.RawMessage) error) error {
	panic("Load should never be called for webhook.Driver")
}

// Prepare returns a zero-valued Prepared.
func (d *transactor) Prepare(req *pm.TransactionRequest_Prepare) (*pm.TransactionResponse_Prepared, error) {
	if d.bodies[0].Len() != 0 {
		panic("d.body.Len() != 0") // Invariant: previous call is finished.
	}
	return &pm.TransactionResponse_Prepared{}, nil
}

// Store invokes the Webhook URL, with a body containing StoreIterator documents.
func (d *transactor) Store(it *lifecycle.StoreIterator) error {
	for it.Next() {
		var b = &d.bodies[it.Binding]

		if b.Len() != 0 {
			b.WriteString(",\n")
		} else {
			b.WriteString("[\n")
		}
		if _, err := b.Write(it.RawJSON); err != nil {
			return err
		}
	}

	for i := range d.bodies {
		d.bodies[i].WriteString("\n]")
	}
	return nil
}

// Commit awaits the completion of the call started in Store.
func (d *transactor) Commit() error {

	for i, address := range d.addresses {
		var address = address.String()
		var body = &d.bodies[i]

		for attempt := 0; true; attempt++ {
			select {
			case <-d.ctx.Done():
				return d.ctx.Err()
			case <-time.After(backoff(attempt)):
				// Fallthrough.
			}

			request, err := http.NewRequest("POST", address, bytes.NewReader(body.Bytes()))
			if err != nil {
				return fmt.Errorf("http.NewRequest(%s): %w", address, err)
			}
			request.Header.Add("Content-Type", "application/json")

			response, err := http.DefaultClient.Do(request)
			if err == nil {
				err = response.Body.Close()
			}
			if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
				err = fmt.Errorf("unexpected webhook response code %d from %s",
					response.StatusCode, address)
			}

			if err == nil {
				body.Reset() // Reset for next use.
				break
			}

			log.WithFields(log.Fields{
				"err":     err,
				"attempt": attempt,
				"address": address,
			}).Error("failed to invoke Webhook (will retry)")
		}
	}
	return nil
}

// Destroy is a no-op.
func (d *transactor) Destroy() {}

func backoff(attempt int) time.Duration {
	switch attempt {
	case 0:
		return 0
	case 1:
		return time.Millisecond * 100
	case 2, 3, 4, 5, 6, 7, 8, 9, 10:
		return time.Second * time.Duration(attempt-1)
	default:
		return 10 * time.Second
	}
}
