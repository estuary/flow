package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// CatalogJSONSchema returns the source catalog JSON schema understood by Flow.
func CatalogJSONSchema() string {
	var svc = newBuildSvc()
	defer svc.finalize()

	svc.sendBytes(uint32(pf.BuildAPI_CATALOG_SCHEMA), nil)

	var _, out, err = svc.poll()
	if err != nil {
		panic(err)
	} else if len(out) != 1 {
		panic("expected 1 output message")
	}
	return string(svc.arenaSlice(out[0]))
}

// BuildCatalog runs the configured build. The provide |client| is used to resolve resource
// fetches.
func BuildCatalog(config pf.BuildAPI_Config, client *http.Client) (hadUserErrors bool, _ error) {
	var svc = newBuildSvc()
	defer svc.finalize()

	if err := svc.sendMessage(uint32(pf.BuildAPI_BEGIN), &config); err != nil {
		panic(err) // Cannot fail to marshal.
	}

	var trampoline = trampolineServer{
		handlers: []trampolineHandler{
			{
				taskCode: uint32(pf.BuildAPI_TRAMPOLINE_FETCH),
				decode: func(request []byte) (interface{}, error) {
					var fetch = new(pf.BuildAPI_Fetch)
					var err = fetch.Unmarshal(request)
					return fetch, err
				},
				exec: func(i interface{}) ([]byte, error) {
					var fetch = i.(*pf.BuildAPI_Fetch)
					log.WithField("url", fetch.ResourceUrl).Debug("fetch requested")

					var resp, err = client.Get(fetch.ResourceUrl)
					var body = bytes.NewBuffer(make([]byte, 4096))
					body.Truncate(taskResponseHeader) // Reserve.

					if err == nil {
						_, err = io.Copy(body, resp.Body)
					}
					if err == nil && resp.StatusCode != 200 && resp.StatusCode != 204 {
						err = fmt.Errorf("unexpected status %d: %s",
							resp.StatusCode,
							body.String()[taskResponseHeader:],
						)
					}
					return body.Bytes(), err
				},
			},
			{
				taskCode: uint32(pf.BuildAPI_TRAMPOLINE_VALIDATE_MATERIALIZATION),
				decode: func(request []byte) (interface{}, error) {
					var fetch = new(materialize.ValidateRequest)
					var err = fetch.Unmarshal(request)
					return fetch, err
				},
				exec: func(i interface{}) ([]byte, error) {
					var request = i.(*materialize.ValidateRequest)
					log.WithField("request", request).Info("materialize validation requested")
					return nil, fmt.Errorf("not yet implemented")
				},
			},
		},
		// resolvedCh is resolved trampoline tasks being sent back to the service.
		resolvedCh: make(chan []byte, 8),
	}
	// mayPoll tracks whether we've resolved tasks since our last poll.
	var mayPoll = true

	for {
		var resolved []byte

		if !mayPoll {
			resolved = <-trampoline.resolvedCh // Must block.
		} else {
			select {
			case resolved = <-trampoline.resolvedCh:
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
			return true, err
		}
		// We must resolve pending work before polling again.
		mayPoll = false

		for _, o := range out {
			switch pf.BuildAPI_Code(o.code) {

			case pf.BuildAPI_DONE:
				return false, nil

			case pf.BuildAPI_DONE_WITH_ERRORS:
				return true, nil

			case pf.BuildAPI_TRAMPOLINE:
				trampoline.startTask(svc.arenaSlice(o))

			default:
				log.WithField("code", o.code).Panic("unexpected code from Rust bindings")
			}
		}
	}
}

func newBuildSvc() *service {
	return newService(
		func() *C.Channel { return C.build_create() },
		func(ch *C.Channel, in C.In1) { C.build_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.build_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.build_invoke16(ch, in) },
		func(ch *C.Channel) { C.build_drop(ch) },
	)
}

type trampolineServer struct {
	handlers   []trampolineHandler
	resolvedCh chan []byte
}

type trampolineHandler struct {
	// Task code handled by this handler.
	taskCode uint32
	// Decode request into a parsed form, which no longer references |request|.
	decode func(request []byte) (interface{}, error)
	// Execute a task, eventually returning a []byte response or error.
	// The []byte response must pre-allocate |taskResponseHeader| bytes of header
	// prefix, which will be filled by the trampolineServer.
	exec func(interface{}) ([]byte, error)
}

func (s trampolineServer) startTask(request []byte) {
	// Task requests are 8 bytes of task ID, followed by 4 bytes of LE task code.
	var taskCode = binary.LittleEndian.Uint32(request[8:12])
	var taskID = binary.LittleEndian.Uint64(request[0:8])
	request = request[12:]

	var decoded interface{}
	var err error
	var exec func(interface{}) ([]byte, error)

	for _, h := range s.handlers {
		if h.taskCode != taskCode {
			continue
		}
		if decoded, err = h.decode(request); err != nil {
			err = fmt.Errorf("decoding trampoline task: %w", err)
		}
		exec = h.exec
		break
	}
	if exec == nil {
		exec = func(interface{}) ([]byte, error) {
			return nil, fmt.Errorf("no handler for task code %d", taskCode)
		}
	}

	log.WithFields(log.Fields{
		"id":      taskID,
		"code":    taskCode,
		"decoded": decoded,
	}).Debug("spawning trampoline task")

	go func() {
		var response []byte
		if err == nil {
			response, err = exec(decoded)
		}
		if err == nil {
			response[8] = 1 // Mark OK.
		} else {
			response = append(make([]byte, taskResponseHeader), err.Error()...)
			response[8] = 0 // Mark !OK.
		}
		binary.LittleEndian.PutUint64(response[:8], taskID)

		log.WithFields(log.Fields{
			"id":   taskID,
			"code": taskCode,
			"err":  err,
		}).Debug("resolving trampoline task")

		s.resolvedCh <- response
	}()
}

// Tasks responses are 8 bytes of task ID, followed by one byte of
// "success" (1) or "failed" (0).
const taskResponseHeader = 9
