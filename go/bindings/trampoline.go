package bindings

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type trampolineServer struct {
	// Context applied to handler invocations.
	ctx context.Context
	// Canceller of |ctx|.
	cancelFn context.CancelFunc
	// Task handlers of the trampoline server.
	handlers []trampolineHandler
	// Channel into which resolved task responses are written.
	resolvedCh chan<- []byte
	// WaitGroup over running tasks.
	wg sync.WaitGroup
}

type trampolineHandler struct {
	// Task code handled by this handler.
	taskCode uint32
	// Decode request into a parsed form, which no longer references |request|.
	decode func(request []byte) (interface{}, error)
	// Execute a task, eventually returning a []byte response or error.
	// The []byte response must pre-allocate |taskResponseHeader| bytes of header
	// prefix, which will be filled by the trampolineServer.
	exec func(context.Context, interface{}) ([]byte, error)
}

// newTrampolineServer returns a trampolineServer prepared with the given context
// and handlers, and a channel from which task resolutions may be read.
func newTrampolineServer(ctx context.Context, handlers ...trampolineHandler) (*trampolineServer, <-chan []byte) {
	var ch = make(chan []byte, 8)
	ctx, cancelFn := context.WithCancel(ctx)

	return &trampolineServer{
		ctx:        ctx,
		cancelFn:   cancelFn,
		handlers:   handlers,
		resolvedCh: ch,
	}, ch
}

// startTask with definition |request|, provided from the Rust side of a CGO bridge.
// |request| is immediately decoded and not retained beyond this call.
// The task handler is executed asynchronously, and eventually writes its task
// response into the channel returned with newTrampolineServer.
func (s *trampolineServer) startTask(request []byte) {
	// Task requests are 8 bytes of task ID, followed by 4 bytes of LE task code.
	var taskCode = binary.LittleEndian.Uint32(request[8:12])
	var taskID = binary.LittleEndian.Uint64(request[0:8])
	request = request[12:]

	var decoded interface{}
	var err error
	var exec func(context.Context, interface{}) ([]byte, error)

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
		exec = func(context.Context, interface{}) ([]byte, error) {
			return nil, fmt.Errorf("no handler for task code %d", taskCode)
		}
	}

	log.WithFields(log.Fields{
		"id":      taskID,
		"code":    taskCode,
		"decoded": decoded,
	}).Trace("serving trampoline task")

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		var response []byte
		if err == nil {
			response, err = exec(s.ctx, decoded)
		}
		if err == nil {
			response[8] = 1 // Mark OK.
		} else {
			response = append(make([]byte, taskResponseHeader), err.Error()...)
			response[8] = 0 // Mark !OK.
		}
		binary.LittleEndian.PutUint64(response[:8], taskID)

		log.WithFields(log.Fields{
			"id":      taskID,
			"code":    taskCode,
			"decoded": decoded,
			"err":     err,
		}).Trace("resolving trampoline task")

		// We _must_ send the response, even if the context has been cancelled.
		// This is because the BuildCatalog function will wait indefinitely on
		// trampoline tasks to complete.
		s.resolvedCh <- response
	}()
}

// Stop the trampoline server, blocking until all tasks complete.
func (s *trampolineServer) stop() {
	s.cancelFn()
	s.wg.Wait()
}

// Tasks responses are 8 bytes of task ID, followed by one byte of
// "success" (1) or "failed" (0).
const taskResponseHeader = 9
