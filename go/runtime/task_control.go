package runtime

import (
	"context"
	"io"
	"strings"

	"github.com/estuary/flow/go/labels"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// taskControl serves TaskControl.SyncNow on the reactor front door, as a
// semantics-free relay: it routes a caller's request to the runtime-next
// sidecar co-located with the task's shard-zero primary, which hosts the
// task's leader session and owns all SyncNow semantics. Callers reach it over
// HTTP/NDJSON; see task_control_http.go.
type taskControl struct {
	service *consumer.Service
	// sidecarEndpoint maps a reactor member endpoint to the dial-able URL of
	// its co-located runtime-next sidecar (FlowConsumerConfig.SidecarEndpoint).
	sidecarEndpoint func(reactor pb.Endpoint) (string, error)
}

// syncNow resolves the task's shard-zero primary, dials its co-located
// sidecar, and relays response messages 1:1 until EOF or error. Any front door
// can dial any sidecar (they listen on a fleet-wide port), so there's no
// gazette-style proxy hop to the primary's own front door. The caller's
// Authorization is forwarded verbatim, for the sidecar to independently verify.
func (tc *taskControl) syncNow(
	ctx context.Context,
	claims pb.Claims,
	req *pr.SyncNowRequest,
	send func(*pr.SyncNowResponse) error,
) error {
	if req.TaskName == "" {
		return status.Error(codes.InvalidArgument, "task_name is required")
	}
	var shardZero, found = taskShardZero(tc.service.State, req.TaskName)
	if !found {
		return errTaskNotFound(req.TaskName)
	}

	// MayProxy because we never serve locally: the sidecar does, whether or not
	// we're the primary.
	var res, err = tc.service.Resolver.Resolve(consumer.ResolveArgs{
		Context:  ctx,
		Claims:   claims,
		ShardID:  shardZero,
		MayProxy: true,
	})
	if err != nil {
		return err
	}
	// Resolution was needed only for routing. Release it now: a SyncNow relay
	// can be open for an hour, and must not pin the local shard's teardown.
	// If the primary moves mid-relay the sidecar stream breaks, and the
	// caller re-invokes (SyncNow is idempotent).
	if res.Done != nil {
		res.Done()
	}

	switch res.Status {
	case pc.Status_OK:
		// Pass.
	case pc.Status_SHARD_NOT_FOUND:
		// Also returned when the caller's claims don't cover the shard.
		return errTaskNotFound(req.TaskName)
	default:
		return status.Errorf(codes.Unavailable,
			"cannot resolve task shard %s to a ready primary (%s)", shardZero, res.Status)
	}

	// Captures and derivations hold no open transaction, so there's nothing to
	// force or await. Only this front door can answer that, because only it
	// resolved the task's actual shard (and thus its type) from the keyspace.
	// Resolve() above verified the caller's claims cover that shard.
	if !strings.HasPrefix(shardZero.String(), "materialize/") {
		if err = send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{}},
		}); err != nil {
			return err
		}
		return send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{}},
		})
	}
	var sidecar string
	if sidecar, err = tc.sidecarEndpoint(res.Header.Route.Endpoints[res.Header.Route.Primary]); err != nil {
		return err
	}
	var conn *grpc.ClientConn
	if conn, err = dialSidecar(ctx, sidecar); err != nil {
		return err
	}
	defer conn.Close()

	var client pr.TaskControl_SyncNowClient
	if client, err = pr.NewTaskControlClient(conn).SyncNow(forwardAuthorization(ctx), req); err != nil {
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

func errTaskNotFound(taskName string) error {
	return status.Errorf(codes.NotFound,
		"task %s was not found in this data plane (or your authorization does not cover it)", taskName)
}

// taskShardZero returns the ID of the named task's shard zero: the shard with
// the lowest key / r-clock range. Shard IDs are `<task-type>/<task-name>/
// <generation-id>/<range>`, so a prefix scan finds the task's shards in
// range-ascending order — but one task's name may prefix another's, so require
// an exact task-name label match.
//
// A `reset` publication starts a new generation, and activation creates the new
// shards before deleting the old, so both can briefly appear here and we take
// the older. Sync-now during a reset is meaningless either way: the task is
// being backfilled from scratch.
func taskShardZero(state *allocator.State, taskName string) (pc.ShardID, bool) {
	state.KS.Mu.RLock()
	defer state.KS.Mu.RUnlock()

	for _, taskType := range []string{"capture", "derivation", "materialize"} {
		var prefix = allocator.ItemKey(state.KS, taskType+"/"+taskName+"/")
		for _, kv := range state.Items.Prefixed(prefix) {
			var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
			if spec.LabelSet.ValueOf(labels.TaskName) != taskName {
				continue
			}
			return spec.Id, true
		}
	}
	return "", false
}

// dialSidecar dials the runtime-next sidecar at `endpoint`, using TLS
// or plaintext per its scheme (which mirrors the reactor's own).
//
// A nil TLS config means the process's ambient roots (honoring SSL_CERT_FILE):
// the same trust `gazette::dial_channel` applies to the sidecar's own leader
// and shuffle dials. The reactor's `--*-ca-file` flags configure gazette
// peers, not this hop.
func dialSidecar(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	var ep = pb.Endpoint(endpoint)
	if err := ep.Validate(); err != nil {
		return nil, err
	}
	var creds credentials.TransportCredentials
	if ep.URL().Scheme == "https" {
		creds = credentials.NewTLS(nil)
	} else {
		creds = insecure.NewCredentials()
	}
	return grpc.DialContext(ctx, ep.GRPCAddr(), grpc.WithTransportCredentials(creds))
}

// forwardAuthorization returns a Context which forwards the caller's incoming
// Authorization verbatim on outgoing RPCs, so that the sidecar independently
// verifies the caller's own token.
func forwardAuthorization(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if auth := md.Get("authorization"); len(auth) != 0 {
			return metadata.AppendToOutgoingContext(ctx, "authorization", auth[len(auth)-1])
		}
	}
	return ctx
}
