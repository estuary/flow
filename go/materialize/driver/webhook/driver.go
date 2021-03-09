package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/estuary/flow/go/materialize/lifecycle"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/protocol"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

// NewDriver returns a new Webhook driver.
func NewDriver() pm.DriverServer { return driver{} }

type config struct {
	Endpoint protocol.Endpoint
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	return c.Endpoint.Validate()
}

// Validate validates the Webhook configuration and constrains projections
// to the document root (only).
func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	var constraints = make(map[string]*pm.Constraint)

	if err := req.Collection.Validate(); err != nil {
		return nil, fmt.Errorf("validating collection: %w", err)
	} else if err = json.Unmarshal([]byte(req.EndpointConfigJson), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	} else if err = cfg.Validate(); err != nil {
		return nil, err
	}

	for _, projection := range req.Collection.Projections {
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

	return &pm.ValidateResponse{Constraints: constraints}, nil
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

	if err := json.Unmarshal([]byte(open.Open.EndpointConfigJson), &cfg); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	} else if err = cfg.Validate(); err != nil {
		return err
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{
			FlowCheckpoint: nil,
			DeltaUpdates:   true,
		},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	var log = log.WithField("endpoint", cfg.Endpoint)
	var transactor = &transactor{ctx: stream.Context(), config: cfg}
	return lifecycle.RunTransactions(stream, transactor, log)
}

type transactor struct {
	ctx context.Context
	config
	body bytes.Buffer
}

// Load should not be called and panics.
func (d *transactor) Load(_ *lifecycle.LoadIterator, _ <-chan struct{}, _ func(json.RawMessage) error) error {
	panic("Load should never be called for webhook.Driver")
}

// Prepare returns a zero-valued Prepared.
func (d *transactor) Prepare(req *pm.TransactionRequest_Prepare) (*pm.TransactionResponse_Prepared, error) {
	if d.body.Len() != 0 {
		panic("d.body.Len() != 0") // Invariant: previous call is finished.
	}
	return &pm.TransactionResponse_Prepared{}, nil
}

// Store invokes the Webhook URL, with a body containing StoreIterator documents.
func (d *transactor) Store(it *lifecycle.StoreIterator) error {
	var comma bool

	for it.Next() {
		if comma {
			d.body.WriteString(",\n")
		} else {
			d.body.WriteString("[\n")
			comma = true
		}
		if _, err := d.body.Write(it.RawJSON); err != nil {
			return err
		}
	}

	d.body.WriteString("\n]")
	return nil
}

// Commit awaits the completion of the call started in Store.
func (d *transactor) Commit() error {
	for attempt := 0; true; attempt++ {
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case <-time.After(backoff(attempt)):
			// Fallthrough.
		}

		request, err := http.NewRequest("POST", string(d.Endpoint), bytes.NewReader(d.body.Bytes()))
		if err != nil {
			return fmt.Errorf("http.NewRequest(%s): %w", d.Endpoint, err)
		}
		request.Header.Add("Content-Type", "application/json")

		response, err := http.DefaultClient.Do(request)
		if err == nil {
			err = response.Body.Close()
		}
		if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
			err = fmt.Errorf("unexpected webhook response code %d from %s",
				response.StatusCode, d.Endpoint)
		}

		if err == nil {
			d.body.Reset()
			return nil
		}

		log.WithFields(log.Fields{
			"err":      err,
			"attempt":  attempt,
			"endpoint": d.Endpoint,
		}).Error("failed to invoke Webhook (will retry)")
	}
	panic("not reached")
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
