package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// syncNowDefaultTimeout bounds a SyncNow that arrives without a client
// deadline, so the handler cannot block indefinitely if a forced transaction
// never commits (e.g. a wedged connector). Clients (flowctl) normally set
// their own, tighter deadline.
const syncNowDefaultTimeout = 5 * time.Minute

// syncNowServer implements the SyncNow API: it forces a task shard to
// immediately commit its open transaction ("sync now"), blocking until the
// commit is durable. Routing to the shard's primary mirrors the Gazette shard
// Stat API, using the same consumer.Resolver as the Shuffler and NetworkProxy.
type syncNowServer struct {
	resolver *consumer.Resolver
}

var _ pf.AuthSyncNowServer = (*syncNowServer)(nil)

// syncNowStore is the subset of a running V2 task application reached via
// consumer.Resolution.Store. Implemented by taskBase (and thus by each V2
// app); the legacy runtime's store does not implement it, which the handler
// maps to NOT_SUPPORTED.
type syncNowStore interface {
	RequestCloseNow(ctx context.Context) error
}

func (s *syncNowServer) SyncNow(claims pb.Claims, ctx context.Context, req *pf.SyncNowRequest) (*pf.SyncNowResponse, error) {
	if err := req.ShardId.Validate(); err != nil {
		return nil, fmt.Errorf("invalid shard id: %w", err)
	}

	// Resolve to the shard's primary. MayProxy is false: we must be the primary
	// ourselves, or else the returned Header carries the route for the client to
	// retry against the current primary (the Gazette route-discovery pattern).
	resolution, err := s.resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		Claims:      claims,
		MayProxy:    false,
		ProxyHeader: req.Header,
		ShardID:     req.ShardId,
	})
	if err != nil {
		return nil, err
	}

	var resp = &pf.SyncNowResponse{
		Status: pf.SyncNowResponse_Status(resolution.Status),
		Header: &resolution.Header,
	}
	if resolution.Status != pc.Status_OK {
		return resp, nil // e.g. NOT_SHARD_PRIMARY: client retries via Header.
	}

	// We are the primary. Reach the running V2 store, or report NOT_SUPPORTED
	// for a legacy-runtime shard whose store has no sync-now path.
	var store, ok = resolution.Store.(syncNowStore)
	resolution.Done()

	if !ok {
		resp.Status = pf.SyncNowResponse_NOT_SUPPORTED
		return resp, nil
	}

	// Bound the wait: honor the client's deadline if any, else a default cap so
	// a wedged transaction cannot block the RPC forever.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, syncNowDefaultTimeout)
		defer cancel()
	}

	switch err := store.RequestCloseNow(ctx); {
	case err == nil:
		resp.Status = pf.SyncNowResponse_OK // Forced transaction committed.
	case errors.Is(err, errSyncNowUnsupported):
		resp.Status = pf.SyncNowResponse_NOT_SUPPORTED
	case errors.Is(err, context.DeadlineExceeded):
		resp.Status = pf.SyncNowResponse_TIMEOUT
	default:
		return resp, err // Client cancellation or an unexpected error.
	}
	return resp, nil
}
