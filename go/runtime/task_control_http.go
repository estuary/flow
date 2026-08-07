package runtime

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/gogo/gateway"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TaskControlSyncNowPath is the reactor front-door HTTP endpoint of
// TaskControl.SyncNow. It's mounted alongside gazette's grpc-gateway
// `/v1/` mux with the same CORS treatment, and speaks the same
// fetch-streaming NDJSON dialect as gateway streaming RPCs
// (such as dashboard journal reads).
const TaskControlSyncNowPath = "/v1/task-control/sync-now"

// taskControlHTTP is the HTTP transport of TaskControl.SyncNow.
//
// This endpoint is the UI contract of the dashboard's "Sync Now" button:
//
//	POST /v1/task-control/sync-now
//	Authorization: Bearer <token>    -- a Read-level `/authorize/user/task`
//	                                    reactor token for the task.
//	Content-Type: application/json
//
//	{"taskName": "acmeCo/my/materialization"}
//
// The response is newline-delimited JSON, flushed per message, where each
// line wraps a runtime.SyncNowResponse exactly as grpc-gateway would:
//
//	{"result": {"ack": {"outcome": "HELD_COLLAPSED", "status": {...}}}}
//	{"result": {"progress": {"sourcedDocsTotal": "10", ...}}}
//	{"result": {"done": {"committedDocsTotal": "10", ...}}}
//
// The stream is exactly one `ack`, then zero or more `progress` heartbeats
// (one every ~15 seconds while awaiting the transaction — consume them as
// progress updates; they also keep idle load-balancer timeouts at bay), then
// exactly one `done`, after which the stream closes. Ack outcomes are
// HELD_COLLAPSED | CLOSE_REQUESTED | ALREADY_CLOSING | IDLE | NOT_APPLICABLE;
// for IDLE and NOT_APPLICABLE there is nothing to await and `done` follows
// immediately. See runtime.proto for full message shapes and semantics.
//
// A terminal error is a final `{"error": {"grpcCode": ..., "httpCode": ...,
// "message": ..., "httpStatus": ...}}` line (grpc-gateway's mid-stream error
// convention), with the HTTP status also set when no `result` line has been
// written yet. Notably `httpCode` 404 means the task isn't running in this
// data plane (or isn't on the V2 runtime, or the token doesn't cover it).
//
// SyncNow is idempotent: once `ack` is received the commit request has
// landed, hanging up early is harmless, and concurrent calls await the same
// commit. If the stream dies before `done` (network hiccup, task
// re-assignment), simply re-invoke.
type taskControlHTTP struct {
	relay    *taskControl
	verifier pb.Verifier
}

// jsonpbMarshaler matches the marshaling of gazette's grpc-gateway mux
// (see runconsumer.Main): camelCase names, enums as strings, zero-valued
// fields emitted.
var jsonpbMarshaler = &gateway.JSONPb{EmitDefaults: true}

func (h *taskControlHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", jsonpbMarshaler.ContentType())

	if req.Method != http.MethodPost {
		writeStreamError(w, false, status.Error(codes.InvalidArgument, "expected POST"))
		return
	}
	var syncNow = new(pr.SyncNowRequest)
	if err := jsonpbMarshaler.NewDecoder(req.Body).Decode(syncNow); err != nil && err != io.EOF {
		writeStreamError(w, false, status.Errorf(codes.InvalidArgument, "parsing request body: %s", err))
		return
	}

	// Verify the caller's token, as the gRPC transport's verified server
	// wrapper does. The relay forwards it verbatim from this Context.
	var ctx = metadata.NewIncomingContext(req.Context(),
		metadata.Pairs("authorization", req.Header.Get("Authorization")))
	ctx, cancel, claims, err := h.verifier.Verify(ctx, pb.Capability_READ)
	if err != nil {
		writeStreamError(w, false, err)
		return
	}
	defer cancel()

	var flusher, _ = w.(http.Flusher)
	var wroteResult = false
	err = h.relay.syncNow(ctx, claims, syncNow, func(resp *pr.SyncNowResponse) error {
		var line, err = jsonpbMarshaler.Marshal(map[string]interface{}{"result": resp})
		if err != nil {
			return err
		}
		if _, err = w.Write(append(line, '\n')); err != nil {
			return err
		}
		wroteResult = true
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	if err != nil && !errors.Is(err, req.Context().Err()) {
		writeStreamError(w, wroteResult, err)
	}
}

// writeStreamError writes a terminal error in grpc-gateway's mid-stream
// convention: an `{"error": {...}}` NDJSON line, preceded by the mapped HTTP
// status code if the response header hasn't already been sent with a result.
func writeStreamError(w http.ResponseWriter, wroteResult bool, err error) {
	var s = status.Convert(err)
	var httpCode = gwruntime.HTTPStatusFromCode(s.Code())

	if !wroteResult {
		w.WriteHeader(httpCode)
	}
	var line, _ = json.Marshal(map[string]interface{}{
		"error": map[string]interface{}{
			"grpcCode":   int32(s.Code()),
			"httpCode":   httpCode,
			"message":    s.Message(),
			"httpStatus": http.StatusText(httpCode),
		},
	})
	_, _ = w.Write(append(line, '\n'))
}
