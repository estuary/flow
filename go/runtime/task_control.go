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

// taskControl serves the runtime.TaskControl service on the reactor front
// door, as a semantics-free relay: it routes a caller's RPC to the
// runtime-next sidecar co-located with the task's shard-zero primary, which
// hosts the task's leader session and owns all SyncNow semantics. Resolution
// follows the gazette mayProxy pattern — if shard zero's primary is a peer
// reactor, the RPC is forwarded once to that peer's front door — and the
// caller's Authorization is forwarded verbatim for the sidecar to
// independently verify.
type taskControl struct {
	service *consumer.Service
	// sidecarEndpoint maps a reactor member endpoint to the dial-able URL of
	// its co-located runtime-next sidecar (FlowConsumerConfig.SidecarEndpoint).
	sidecarEndpoint func(reactor pb.Endpoint) (string, error)
}

var _ pr.AuthTaskControlServer = &taskControl{}

func (tc *taskControl) SyncNow(claims pb.Claims, req *pr.SyncNowRequest, stream pr.TaskControl_SyncNowServer) error {
	return tc.syncNow(stream.Context(), claims, req, stream.Send)
}

// syncNow is the relay core shared by the gRPC and HTTP transports:
// resolve the task's shard-zero primary, dial its co-located sidecar
// (or forward to the peer reactor's front door), and relay response
// messages 1:1 until EOF or error.
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

	res, err := tc.service.Resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		Claims:      claims,
		ShardID:     shardZero,
		MayProxy:    proxyHeader == nil, // MayProxy if request hasn't already been proxied.
		ProxyHeader: proxyHeader,
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
			Response: &pr.SyncNowResponse_Ack_{Ack: &pr.SyncNowResponse_Ack{
				Outcome: pr.SyncNowResponse_NOT_APPLICABLE,
				Status:  &pr.SyncNowResponse_Status{},
			}},
		}); err != nil {
			return err
		}
		return send(&pr.SyncNowResponse{
			Response: &pr.SyncNowResponse_Done_{Done: &pr.SyncNowResponse_Done{}},
		})
	}
	ctx = forwardAuthorization(ctx)

	var client pr.TaskControl_SyncNowClient
	if res.Store == nil {
		// The primary is a peer reactor. Forward this RPC to its front door.
		ctx = attachProxyHeader(ctx, &res.Header)
		ctx = pb.WithDispatchRoute(ctx, res.Header.Route, res.Header.ProcessId)
		client, err = pr.NewTaskControlClient(tc.service.Loopback).SyncNow(ctx, req)
	} else {
		// We are the primary. Dial our co-located sidecar, which hosts the
		// task's leader session.
		var primary = res.Header.Route.Endpoints[res.Header.Route.Primary]
		var sidecar string
		if sidecar, err = tc.sidecarEndpoint(primary); err != nil {
			return err
		}
		var conn *grpc.ClientConn
		if conn, err = dialSidecar(ctx, sidecar); err != nil {
			return err
		}
		defer conn.Close()
		client, err = pr.NewTaskControlClient(conn).SyncNow(ctx, req)
	}
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
// Authorization verbatim on outgoing RPCs, so the next hop (peer reactor or
// sidecar) independently verifies the caller's own token.
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
