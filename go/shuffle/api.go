package shuffle

import (
	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// API is the server side implementation of the Shuffle protocol.
type API struct {
	// resolve is a consumer.Resolver.Resolve() closure, stubbed for easier testing.
	resolve func(consumer.ResolveArgs) (consumer.Resolution, error)
}

// Shuffle implements the gRPC Shuffle endpoint.
func (api *API) Shuffle(req *pf.ShuffleRequest, stream pf.Shuffler_ShuffleServer) error {
	if err := req.Validate(); err != nil {
		return err
	}
	var res, err = api.resolve(consumer.ResolveArgs{
		Context:     stream.Context(),
		ShardID:     req.Shuffle.Coordinator,
		MayProxy:    false,
		ProxyHeader: req.Resolution,
	})
	var resp = pf.ShuffleResponse{
		Status: res.Status,
		Header: &res.Header,
	}

	if err != nil {
		return err
	} else if resp.Status != pc.Status_OK {
		return stream.SendMsg(&resp)
	}
	defer res.Done()

	// API requires that the consumer.Store be able to provide a Coordinator.
	var coordinator = res.Store.(interface{ Coordinator() *coordinator }).Coordinator()
	var ring = coordinator.findOrCreateRing(req.Shuffle)
	var doneCh = make(chan error, 1)

	ring.subscriberCh <- subscriber{
		ShuffleRequest: *req,
		sendMsg:        stream.SendMsg,
		sendCtx:        stream.Context(),
		doneCh:         doneCh,
	}
	err = <-doneCh

	if stream.Context().Err() != nil {
		err = nil // Peer cancellations are not an error.
	} else if err != nil {
		// We got an error on SendMsg to the peer, which as-implemented by gRPC is always an EOF.
		err = stream.RecvMsg(new(pf.ShuffleRequest)) // Read a more descriptive error.

		log.WithFields(log.Fields{
			"err":     err,
			"journal": req.Shuffle.Journal,
			"range":   req.Range,
		}).Warn("failed to send ShuffleResponse to client")
	}
	return err
}
