package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// CatalogJSONSchema returns the source catalog JSON schema understood by Flow.
func CatalogJSONSchema() string {
	var publisher = ops.NewLocalPublisher(labels.ShardLabeling{})
	var svc, err = newBuildSvc(publisher)
	if err != nil {
		panic(err)
	}
	defer svc.destroy()

	svc.sendBytes(uint32(pf.BuildAPI_CATALOG_SCHEMA), nil)

	_, out, err := svc.poll()
	if err != nil {
		panic(err)
	} else if len(out) != 1 {
		panic("expected 1 output message")
	}
	return string(svc.arenaSlice(out[0]))
}

// BuildArgs are arguments of the BuildCatalog function.
type BuildArgs struct {
	pf.BuildAPI_Config
	// Context of asynchronous tasks undertaken during the build.
	Context context.Context
	// Directory which roots fetched file:// resolutions.
	// Or empty, if file:// resolutions are disallowed.
	FileRoot string
	// Publisher of operation logs and stats to use.
	// If not set, a publisher will be created that logs to stderr.
	OpsPublisher ops.Publisher
}

// BuildCatalog runs the configured build.
func BuildCatalog(args BuildArgs) error {
	if err := args.BuildAPI_Config.Validate(); err != nil {
		return fmt.Errorf("validating configuration: %w", err)
	}

	var transport = http.DefaultTransport.(*http.Transport).Clone()
	var client = &http.Client{Transport: transport}

	if args.FileRoot != "" {
		transport.RegisterProtocol("file", http.NewFileTransport(http.Dir(args.FileRoot)))
	}
	if args.OpsPublisher == nil {
		args.OpsPublisher = ops.NewLocalPublisher(labels.ShardLabeling{
			Build: args.BuildId,
		})
	}

	var svc, err = newBuildSvc(args.OpsPublisher)
	if err != nil {
		return fmt.Errorf("creating build service: %w", err)
	}
	defer svc.destroy()

	if err := svc.sendMessage(uint32(pf.BuildAPI_BEGIN), &args.BuildAPI_Config); err != nil {
		panic(err) // Cannot fail to marshal.
	}

	var trampoline, resolvedCh = newTrampolineServer(
		args.Context,
		trampolineHandler{
			taskCode: uint32(pf.BuildAPI_TRAMPOLINE_FETCH),
			decode: func(request []byte) (interface{}, error) {
				var fetch = new(pf.BuildAPI_Fetch)
				var err = fetch.Unmarshal(request)
				return fetch, err
			},
			exec: func(ctx context.Context, i interface{}) ([]byte, error) {
				var fetch = i.(*pf.BuildAPI_Fetch)

				var body = bytes.NewBuffer(make([]byte, 4096))
				body.Truncate(taskResponseHeader) // Reserve.

				var req, err = http.NewRequestWithContext(ctx, "GET", fetch.ResourceUrl, nil)
				var resp *http.Response

				if err == nil {
					resp, err = client.Do(req)
				}
				if err == nil {
					_, err = io.Copy(body, resp.Body)
				}
				if err == nil && resp.StatusCode != 200 && resp.StatusCode != 204 {
					err = fmt.Errorf("unexpected status %d: %s",
						resp.StatusCode,
						strings.TrimSpace(body.String()[taskResponseHeader:]),
					)
				}
				return body.Bytes(), err
			},
		},
		trampolineHandler{
			taskCode: uint32(pf.BuildAPI_TRAMPOLINE_VALIDATE_CAPTURE),
			decode: func(request []byte) (interface{}, error) {
				var m = new(pc.ValidateRequest)
				var err = m.Unmarshal(request)
				return m, err
			},
			exec: func(ctx context.Context, i interface{}) ([]byte, error) {
				var request = i.(*pc.ValidateRequest)
				log.WithField("request", request).Debug("capture validation requested")

				var response, err = connector.Invoke(
					ctx,
					request,
					args.BuildAPI_Config.ConnectorNetwork,
					args.OpsPublisher,
					func(driver *connector.Driver, request *pc.ValidateRequest) (*pc.ValidateResponse, error) {
						return driver.CaptureClient().Validate(ctx, request)
					},
				)
				if err != nil {
					return nil, err
				}
				log.WithField("response", response).Debug("capture validation response")

				// Return marshaled response with a |taskResponseHeader| prefix.
				var out = make([]byte, taskResponseHeader+response.ProtoSize())
				if _, err = response.MarshalTo(out[taskResponseHeader:]); err != nil {
					return nil, fmt.Errorf("marshal response: %w", err)
				}
				return out, err
			},
		},
		trampolineHandler{
			taskCode: uint32(pf.BuildAPI_TRAMPOLINE_VALIDATE_MATERIALIZATION),
			decode: func(request []byte) (interface{}, error) {
				var m = new(pm.ValidateRequest)
				var err = m.Unmarshal(request)
				return m, err
			},
			exec: func(ctx context.Context, i interface{}) ([]byte, error) {
				var request = i.(*pm.ValidateRequest)
				log.WithField("request", request).Debug("materialize validation requested")

				var response, err = connector.Invoke(
					ctx,
					request,
					args.BuildAPI_Config.ConnectorNetwork,
					args.OpsPublisher,
					func(driver *connector.Driver, request *pm.ValidateRequest) (*pm.ValidateResponse, error) {
						return driver.MaterializeClient().Validate(ctx, request)
					},
				)
				if err != nil {
					return nil, err
				}
				log.WithField("response", response).Debug("materialize validation response")

				// Return marshaled response with a |taskResponseHeader| prefix.
				var out = make([]byte, taskResponseHeader+response.ProtoSize())
				if _, err = response.MarshalTo(out[taskResponseHeader:]); err != nil {
					return nil, fmt.Errorf("marshal response: %w", err)
				}
				return out, err
			},
		})
	defer trampoline.stop()

	// mayPoll tracks whether we've resolved tasks since our last poll.
	var mayPoll = true

	for {
		var resolved []byte

		if !mayPoll {
			resolved = <-resolvedCh // Must block.
		} else {
			select {
			case resolved = <-resolvedCh:
			default: // Don't block.
			}
		}

		if resolved != nil {
			svc.sendBytes(uint32(pf.BuildAPI_TRAMPOLINE), resolved)
			mayPoll = true
			continue
		}

		// Poll the service.
		svc.sendBytes(uint32(pf.BuildAPI_POLL), nil)
		var _, out, err = svc.poll()
		if err != nil {
			return err
		}
		// We must resolve pending work before polling again.
		mayPoll = false

		for _, o := range out {
			switch pf.BuildAPI_Code(o.code) {

			case pf.BuildAPI_DONE, pf.BuildAPI_DONE_WITH_ERRORS:
				return nil

			case pf.BuildAPI_TRAMPOLINE:
				trampoline.startTask(svc.arenaSlice(o))

			default:
				log.WithField("code", o.code).Panic("unexpected code from Rust bindings")
			}
		}
	}

}

func newBuildSvc(publisher ops.Publisher) (*service, error) {
	return newService(
		"build",
		func(logFilter, logDest C.int32_t) *C.Channel { return C.build_create(logFilter, logDest) },
		func(ch *C.Channel, in C.In1) { C.build_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.build_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.build_invoke16(ch, in) },
		func(ch *C.Channel) { C.build_drop(ch) },
		publisher,
	)
}
