package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"

	"github.com/estuary/flow/go/connector"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
)

// CatalogJSONSchema returns the source catalog JSON schema understood by Flow.
func CatalogJSONSchema() string {
	var publisher = ops.NewLocalPublisher(ops.ShardLabeling{})
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
		args.OpsPublisher = ops.NewLocalPublisher(ops.ShardLabeling{
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
				var m = new(pc.Request_Validate)
				var err = m.Unmarshal(request)
				return m, err
			},
			exec: func(ctx context.Context, i interface{}) ([]byte, error) {
				var request = i.(*pc.Request_Validate)
				log.WithField("request", request).Debug("capture validation requested")

				var response, err = connector.Invoke[pc.Response](
					ctx,
					&pc.Request{Validate: request},
					args.BuildAPI_Config.ConnectorNetwork,
					args.OpsPublisher,
					func(driver *connector.Driver) (pc.Connector_CaptureClient, error) {
						return driver.CaptureClient().Capture(ctx)
					},
				)
				if err != nil {
					return nil, err
				}
				log.WithField("response", response).Debug("capture validation response")
				var validated = response.Validated

				// Return marshaled response with a |taskResponseHeader| prefix.
				var out = make([]byte, taskResponseHeader+validated.ProtoSize())
				if _, err = validated.MarshalTo(out[taskResponseHeader:]); err != nil {
					return nil, fmt.Errorf("marshal response: %w", err)
				}
				return out, err
			},
		},
		trampolineHandler{
			taskCode: uint32(pf.BuildAPI_TRAMPOLINE_VALIDATE_MATERIALIZATION),
			decode: func(request []byte) (interface{}, error) {
				var m = new(pm.Request_Validate)
				var err = m.Unmarshal(request)
				return m, err
			},
			exec: func(ctx context.Context, i interface{}) ([]byte, error) {
				var request = i.(*pm.Request_Validate)
				log.WithField("request", request).Debug("materialize validation requested")

				var response, err = connector.Invoke[pm.Response](
					ctx,
					&pm.Request{Validate: request},
					args.BuildAPI_Config.ConnectorNetwork,
					args.OpsPublisher,
					func(driver *connector.Driver) (pm.Connector_MaterializeClient, error) {
						// TODO(johnny): This is to make the gRPC loopback used by sqlite.InProcessServer
						// work properly, and can be removed once that implementation is removed.
						ctx = pb.WithDispatchDefault(ctx)
						return driver.MaterializeClient().Materialize(ctx)
					},
				)
				if err != nil {
					return nil, err
				}
				log.WithField("response", response).Debug("materialize validation response")
				var validated = response.Validated

				// Return marshaled response with a |taskResponseHeader| prefix.
				var out = make([]byte, taskResponseHeader+validated.ProtoSize())
				if _, err = validated.MarshalTo(out[taskResponseHeader:]); err != nil {
					return nil, fmt.Errorf("marshal response: %w", err)
				}
				return out, err
			},
		},
		trampolineHandler{
			taskCode: uint32(pf.BuildAPI_TRAMPOLINE_DOCKER_INSPECT),
			decode: func(request []byte) (interface{}, error) {
				return string(request), nil
			},
			exec: func(ctx context.Context, i interface{}) ([]byte, error) {
				var image = i.(string)
				// We first need to pull the image, since it may not be available locally
				if err := connector.PullImage(ctx, image); err != nil {
					return nil, fmt.Errorf("pulling image: '%s': %w", image, err)
				}

				var cmd = exec.Command("docker", "inspect", image)
				var result, err = cmd.Output()
				if err != nil {
					return nil, fmt.Errorf("invoking docker inspect: %w", err)
				}

				var out = make([]byte, taskResponseHeader+len(result))
				copy(out[taskResponseHeader:], result)
				return out, nil
			},
		},
	)
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
