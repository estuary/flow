package broker

import (
	"context"
	"net"

	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	pbx "go.gazette.dev/core/broker/protocol/ext"
	"go.gazette.dev/core/broker/stores"
	"google.golang.org/grpc/peer"
)

// FragmentStoreHealth dispatches the JournalServer.FragmentStoreHealth API.
func (svc *Service) FragmentStoreHealth(ctx context.Context, claims pb.Claims, req *pb.FragmentStoreHealthRequest) (resp *pb.FragmentStoreHealthResponse, err error) {
	defer instrumentJournalServerRPC("FragmentStoreHealth", &err, nil)()

	defer func() {
		if err != nil {
			var addr net.Addr
			if p, ok := peer.FromContext(ctx); ok {
				addr = p.Addr
			}
			log.WithFields(log.Fields{"err": err, "req": req, "client": addr}).
				Warn("served FragmentStoreHealth RPC failed")
		}
	}()

	// Validate the request.
	if err = req.Validate(); err != nil {
		return nil, err
	}

	svc.resolver.state.KS.Mu.RLock()
	resp = &pb.FragmentStoreHealthResponse{
		Status: pb.Status_OK,
		Header: pbx.NewUnroutedHeader(svc.resolver.state),
	}
	svc.resolver.state.KS.Mu.RUnlock()

	// Get or create an *ActiveStore for the FragmentStore.
	var activeStore = stores.Get(req.FragmentStore)
	var nextCh, healthErr = activeStore.HealthStatus()

	// Await a very first health check.
	if healthErr == stores.ErrFirstHealthCheck {
		select {
		case <-nextCh:
			_, healthErr = activeStore.HealthStatus()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if healthErr != nil {
		resp.Status = pb.Status_FRAGMENT_STORE_UNHEALTHY
		resp.StoreHealthError = healthErr.Error()
	}
	return resp, nil
}
