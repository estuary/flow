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

// syncNow is the relay core shared by the gRPC and HTTP transports: locate
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
	var shardZero, primary, err = taskShardZero(tc.service.State, claims, req.TaskName)
	if err != nil {
		return err
	}

	// Captures and derivations hold no open transaction, so there's nothing to
	// force or await, and no need to route any further.
	if shardZero.LabelSet.ValueOf(labels.TaskType) != ops.TaskType_materialization.String() {
		if err = send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{}},
		}); err != nil {
			return err
		}
		return send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{}},
		})
	}

	if primary.Primary == -1 {
		// Shard zero's primary is a peer reactor, and only its front door
		// holds the shard's session with the task's leader. Forward there.
		ctx = pb.WithDispatchRoute(forwardAuthorization(ctx), primary, primary.Members[0])

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

	// We are the primary. Resolve only to obtain the shard's Store once it's
	// done recovering, and release the resolution before awaiting anything: a
	// wait can run for an hour and must not pin the shard's teardown. Teardown
	// instead ends the leader session, which breaks the barrier and errors the
	// wait; the caller re-invokes.
	res, err := tc.service.Resolver.Resolve(consumer.ResolveArgs{
		Context: ctx,
		Claims:  claims,
		ShardID: shardZero.Id,
	})
	if err != nil {
		return err
	} else if res.Status != pc.Status_OK {
		return status.Errorf(codes.Unavailable,
			"cannot resolve task shard %s to a ready primary (%s)", shardZero.Id, res.Status)
	}
	var app, isV2 = res.Store.(syncNower)
	res.Done()

	if !isV2 {
		return status.Errorf(codes.NotFound,
			"task %s is not running on the V2 runtime, which sync-now requires", req.TaskName)
	}
	return app.syncNow(ctx, send)
}

// taskShardZero returns the spec of the named task's shard zero, and a
// one-member Route to its current primary, which is marked Primary if it's
// the local reactor. Shard zero is the shard with the lowest key / r-clock
// range. Shard IDs are `<task-type>/<task-name>/<generation-id>/<range>`, so a
// prefix scan finds the task's shards in range-ascending order — but one
// task's name may prefix another's, so require an exact task-name label match.
//
// A `reset` publication starts a new generation, and activation creates the new
// shards before deleting the old, so both can briefly appear here and we take
// the older. Sync-now during a reset is meaningless either way: the task is
// being backfilled from scratch.
func taskShardZero(state *allocator.State, claims pb.Claims, taskName string) (*pc.ShardSpec, pb.Route, error) {
	state.KS.Mu.RLock()
	defer state.KS.Mu.RUnlock()

	var spec *pc.ShardSpec
	for _, taskType := range []string{"capture", "derivation", "materialize"} {
		// Not allocator.ItemKey, which panics on characters (such as a space)
		// that a malformed user-supplied task name could contain. A prefix
		// built from such a name simply matches nothing.
		var prefix = state.KS.Root + allocator.ItemsPrefix + taskType + "/" + taskName + "/"
		for _, kv := range state.Items.Prefixed(prefix) {
			var candidate = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
			if candidate.LabelSet.ValueOf(labels.TaskName) == taskName {
				spec = candidate
				break
			}
		}
		if spec != nil {
			break
		}
	}
	// Act as if an unauthorized shard doesn't exist, as Resolve does.
	if spec == nil || !claims.Selector.Matches(spec.LabelSetExt(pb.LabelSet{})) {
		return nil, pb.Route{}, status.Errorf(codes.NotFound,
			"task %s was not found in this data plane (or your authorization does not cover it)", taskName)
	}

	var asn, _, ok = primaryAssignment(state, spec.Id)
	if !ok || state.LocalMemberInd == -1 {
		return nil, pb.Route{}, status.Errorf(codes.Unavailable,
			"task shard %s has no ready primary", spec.Id)
	}
	var id = pb.ProcessSpec_ID{Zone: asn.MemberZone, Suffix: asn.MemberSuffix}
	var endpoint, found = memberEndpoint(state, id)
	if !found {
		return nil, pb.Route{}, status.Errorf(codes.Unavailable,
			"primary reactor %s of task shard %s is not present in Etcd", &id, spec.Id)
	}
	var route = pb.Route{Members: []pb.ProcessSpec_ID{id}, Endpoints: []pb.Endpoint{endpoint}, Primary: -1}
	if id == state.Members[state.LocalMemberInd].Decoded.(allocator.Member).MemberValue.(*pc.ConsumerSpec).Id {
		route.Primary = 0
	}
	return spec, route, nil
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
