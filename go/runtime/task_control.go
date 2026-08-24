package runtime

import (
	"context"
	"io"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// taskControl serves the runtime.TaskControl service on the reactor front
// door. SyncNow is answered by the front door of the task's shard-zero
// primary, which drives the CloseNow / Synced commit barrier over that shard's
// session with the task's leader (see materializeAppV2.syncNow). A front door
// which is not that primary forwards the RPC to the one which is, following
// the gazette mayProxy pattern.
//
// Users reach the service over HTTP/NDJSON; see task_control_http.go. gRPC
// carries only the internal front-door-to-front-door proxy hop.
type taskControl struct {
	service *consumer.Service
}

// syncNower is implemented by the local Store of a task shard which can force
// and await a commit: the V2 runtime's materialization application. A
// materialization whose Store does not implement it runs the V1 runtime,
// which has no leader to ask.
type syncNower interface {
	syncNow(ctx context.Context, send func(*pr.SyncNowResponse) error) error
}

var _ pr.AuthTaskControlServer = &taskControl{}

func (tc *taskControl) SyncNow(claims pb.Claims, req *pr.SyncNowRequest, stream pr.TaskControl_SyncNowServer) error {
	return tc.syncNow(stream.Context(), claims, req, stream.Send)
}

// syncNow is the relay core shared by the gRPC and HTTP transports: resolve
// the task's shard zero, then either await its commit locally or forward to
// the peer front door which is its primary.
func (tc *taskControl) syncNow(
	ctx context.Context,
	claims pb.Claims,
	req *pr.SyncNowRequest,
	send func(*pr.SyncNowResponse) error,
) error {
	if req.TaskName == "" {
		return status.Error(codes.InvalidArgument, "task_name is required")
	}
	var proxyHeader, err = getProxyHeader(ctx)
	if err != nil {
		return err
	}
	var shardZero, found = taskShardZero(tc.service.State, req.TaskName)
	if !found {
		return errTaskNotFound(req.TaskName)
	}

	var res consumer.Resolution
	if res, err = tc.service.Resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		Claims:      claims,
		ShardID:     shardZero.Id,
		MayProxy:    proxyHeader == nil, // MayProxy if not already proxied.
		ProxyHeader: proxyHeader,
	}); err != nil {
		return err
	}

	switch res.Status {
	case pc.Status_OK:
		// Pass.
	case pc.Status_SHARD_NOT_FOUND:
		// Also returned when the caller's claims don't cover the shard.
		return errTaskNotFound(req.TaskName)
	default:
		return status.Errorf(codes.Unavailable,
			"cannot resolve task shard %s to a ready primary (%s)", shardZero.Id, res.Status)
	}

	// Captures and derivations hold no open transaction, so there's nothing to
	// force or await, and no need to route any further. Resolve() verified the
	// caller's claims cover the shard.
	if shardZero.LabelSet.ValueOf(labels.TaskType) != ops.TaskType_materialization.String() {
		if res.Done != nil {
			res.Done()
		}
		if err = send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{}},
		}); err != nil {
			return err
		}
		return send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{}},
		})
	}

	if res.Store == nil {
		// Shard zero's primary is a peer reactor, and only its front door
		// holds the shard's session with the task's leader. Forward there.
		ctx = attachProxyHeader(forwardAuthorization(ctx), &res.Header)
		ctx = pb.WithDispatchRoute(ctx, res.Header.Route, res.Header.ProcessId)

		var client, err = pr.NewTaskControlClient(tc.service.Loopback).SyncNow(ctx, req)
		if err != nil {
			return err
		}
		for {
			var resp, err = client.Recv()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err // Terminal gRPC status passes through verbatim.
			} else if err = send(resp); err != nil {
				return err
			}
		}
	}

	// We are the primary. Take the shard's Store and release the resolution
	// before awaiting anything: a wait can run for an hour and must not pin
	// the shard's teardown. Teardown instead ends the leader session, which
	// breaks the barrier and errors the wait; the caller re-invokes.
	var app, isV2 = res.Store.(syncNower)
	res.Done()

	if !isV2 {
		return status.Errorf(codes.NotFound,
			"task %s is not running on the V2 runtime, which sync-now requires", req.TaskName)
	}
	return app.syncNow(ctx, send)
}

func errTaskNotFound(taskName string) error {
	return status.Errorf(codes.NotFound,
		"task %s was not found in this data plane (or your authorization does not cover it)", taskName)
}

// taskShardZero returns the spec of the named task's shard zero: the shard
// with the lowest key / r-clock range. Shard IDs are `<task-type>/<task-name>/
// <generation-id>/<range>`, so a prefix scan finds the task's shards in
// range-ascending order — but one task's name may prefix another's, so require
// an exact task-name label match.
//
// A `reset` publication starts a new generation, and activation creates the new
// shards before deleting the old, so both can briefly appear here and we take
// the older. Sync-now during a reset is meaningless either way: the task is
// being backfilled from scratch.
func taskShardZero(state *allocator.State, taskName string) (*pc.ShardSpec, bool) {
	state.KS.Mu.RLock()
	defer state.KS.Mu.RUnlock()

	for _, taskType := range []string{"capture", "derivation", "materialize"} {
		var prefix = allocator.ItemKey(state.KS, taskType+"/"+taskName+"/")
		for _, kv := range state.Items.Prefixed(prefix) {
			var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
			if spec.LabelSet.ValueOf(labels.TaskName) != taskName {
				continue
			}
			return spec, true
		}
	}
	return nil, false
}

// forwardAuthorization returns a Context which forwards the caller's incoming
// Authorization verbatim on outgoing RPCs, so that the peer front door
// independently verifies the caller's own token.
func forwardAuthorization(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if auth := md.Get("authorization"); len(auth) != 0 {
			return metadata.AppendToOutgoingContext(ctx, "authorization", auth[len(auth)-1])
		}
	}
	return ctx
}

// taskControlProxyMD carries a marshalled pb.Header on a proxied TaskControl
// RPC. SyncNowRequest deliberately has no Header field (it's user-facing),
// so the gazette proxy convention — Header marks an already-proxied request,
// and synchronizes the peer to the proxying member's Etcd revision — rides
// gRPC metadata instead.
const taskControlProxyMD = "flow-task-control-proxy-bin"

func attachProxyHeader(ctx context.Context, hdr *pb.Header) context.Context {
	var b, err = hdr.Marshal()
	if err != nil {
		panic(err) // Marshal of a valid Header cannot fail.
	}
	return metadata.AppendToOutgoingContext(ctx, taskControlProxyMD, string(b))
}

func getProxyHeader(ctx context.Context) (*pb.Header, error) {
	var md, ok = metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, nil
	}
	var vals = md.Get(taskControlProxyMD)
	if len(vals) == 0 {
		return nil, nil
	}
	var hdr = new(pb.Header)
	if err := hdr.Unmarshal([]byte(vals[len(vals)-1])); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid proxy header: %s", err)
	}
	return hdr, nil
}
