package jsonimage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/estuary/protocols/fdb/tuple"
	pm "github.com/estuary/protocols/materialize"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

// NewDriver returns a new JSON docker image driver.
func NewDriver() pm.DriverServer { return driver{} }

type config struct {
	Image string
}

func newConfig(spec json.RawMessage) (*config, error) {
	var cfg = new(config)

	if err := json.Unmarshal([]byte(spec), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if cfg.Image == "" {
		return nil, fmt.Errorf("expected configuration Image")
	}

	return cfg, nil
}

// Spec returns the specification of the connector.
func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	var cfg, err = newConfig(req.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	type respType struct {
		Spec *pm.SpecResponse `json:"spec"`
	}
	var resp *pm.SpecResponse

	err = invokeConnector(ctx, cfg.Image, nil,
		func(w io.Writer) error {
			return json.NewEncoder(w).Encode(struct {
				Spec *pm.SpecRequest `json:"spec"`
			}{req})
		},
		func() interface{} { return new(respType) },
		func(i interface{}) error { resp = i.(*respType).Spec; return nil },
	)
	return resp, err
}

// Validate validates the configuration.
func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg, err = newConfig(req.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	type respType struct {
		Validated *pm.ValidateResponse `json:"validated"`
	}
	var resp *pm.ValidateResponse

	err = invokeConnector(ctx, cfg.Image, nil,
		func(w io.Writer) error {
			return json.NewEncoder(w).Encode(struct {
				Validate *pm.ValidateRequest `json:"validate"`
			}{req})
		},
		func() interface{} { return new(respType) },
		func(i interface{}) error { resp = i.(*respType).Validated; return nil },
	)
	return resp, err
}

// Apply applies the configuration.
func (driver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var cfg, err = newConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	type respType struct {
		Applied *pm.ApplyResponse `json:"applied"`
	}
	var resp *pm.ApplyResponse

	err = invokeConnector(ctx, cfg.Image, nil,
		func(w io.Writer) error {
			return json.NewEncoder(w).Encode(struct {
				Apply *pm.ApplyRequest `json:"apply"`
			}{req})
		},
		func() interface{} { return new(respType) },
		func(i interface{}) error { resp = i.(*respType).Applied; return nil },
	)
	return resp, err
}

// Transactions implements the DriverServer interface.
func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	}

	cfg, err := newConfig(open.Open.Materialization.EndpointSpecJson)
	if err != nil {
		return err
	}

	// Service loop which reads |stream| and proxies to the container.
	var writeLoop = func(w io.Writer) error {
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

	// Handler which proxies container writes to |stream|.
	var response *pm.TransactionResponse
	var responseHandler = func(i interface{}) error {
		var r = i.(*TxnResponse)

		if r.Opened != nil {
			return pm.WriteOpened(
				stream,
				&response,
				r.Opened,
			)
		} else if r.Loaded != nil {
			return pm.StageLoaded(stream, &response, r.Loaded.Binding, r.Loaded.Document)
		} else if r.Prepared != nil {
			return pm.WritePrepared(stream, &response, r.Prepared)
		} else if r.Committed != nil {
			return pm.WriteCommitted(stream, &response)
		} else {
			return fmt.Errorf("unexpected connector output record: %#v", r)
		}
	}

	return invokeConnector(stream.Context(), cfg.Image, nil,
		writeLoop,
		func() interface{} { return new(TxnResponse) },
		responseHandler,
	)
}

func invokeConnector(
	ctx context.Context,
	image string,
	args []string,
	writeLoop func(io.Writer) error,
	newRecord func() interface{},
	onRecord func(interface{}) error,
) error {

	/*
		return exec.Command(
			"docker",
			"run",
			"--rm",
			c.Image,
		)
	*/
	var parts = strings.Split(image, " ")
	var cmd = exec.Command(parts[0], parts[1:]...)

	var fe = new(firstError)

	// On context cancellation, signal the connector to exit.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-ctx.Done()
		_ = cmd.Process.Signal(syscall.SIGTERM) // TODO: docker stop with timeout
	}()

	// Copy |writeLoop| into connector stdin.
	var wc, err = cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("StdinPipe: %w", err)
	}
	go func() {
		defer wc.Close()
		fe.onError(writeLoop(wc))
	}()

	// Decode and forward connector stdout to |onRecord|.
	cmd.Stdout = &connectorStdout{
		onNew:    newRecord,
		onDecode: onRecord,
		onError: func(err error) {
			fe.onError(err)
			cancel() // Signal to exit.
		},
	}

	fe.onError(cmd.Run())
	_ = cmd.Stdout.(io.Closer).Close()

	return fe.unwrap()
}

type connectorStdout struct {
	rem      []byte
	onNew    func() interface{}
	onDecode func(interface{}) error
	onError  func(error)
}

func (r *connectorStdout) Write(p []byte) (int, error) {
	if len(r.rem) == 0 {
		r.rem = append([]byte(nil), p...) // Clone.
	} else {
		r.rem = append(r.rem, p...)
	}

	var ind = bytes.LastIndexByte(r.rem, '\n') + 1
	var chunk = r.rem[:ind]
	r.rem = r.rem[ind:]

	var dec = json.NewDecoder(bytes.NewReader(chunk))
	dec.DisallowUnknownFields()

	for {
		var rec = r.onNew()

		if err := dec.Decode(rec); err == io.EOF {
			return len(p), nil
		} else if err != nil {
			r.onError(fmt.Errorf("decoding connector record: %w", err))
			return len(p), nil
		} else if err = r.onDecode(rec); err != nil {
			r.onError(err)
			return len(p), nil
		}
	}
}

func (r *connectorStdout) Close() error {
	if len(r.rem) != 0 {
		r.onError(fmt.Errorf("connector stdout closed without a final newline: %q", string(r.rem)))
	}
	return nil
}

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

type firstError struct {
	err error
	mu  sync.Mutex
}

func (fe *firstError) onError(err error) {
	defer fe.mu.Unlock()
	fe.mu.Lock()

	if fe.err == nil {
		fe.err = err
	}
}

func (fe *firstError) unwrap() error {
	defer fe.mu.Unlock()
	fe.mu.Lock()

	return fe.err
}
