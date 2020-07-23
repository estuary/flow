package shuffle

import (
	pf "github.com/estuary/flow/go/protocol"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// API is the server side implementation of the Shuffle protocol.
type API struct {
	resolver *consumer.Resolver

	// TODO(johnny): this is here just for testing, and will be removed when corresponding
	// tests are lifted to a proper E2E consumer integration tests (which requires wiring)
	// up the consumer.Store to host a *coordinator.
	fooCoordinator *coordinator
}

// Shuffle implements the gRPC Shuffle endpoint.
func (api *API) Shuffle(req *pf.ShuffleRequest, stream pf.Shuffler_ShuffleServer) error {
	if err := req.Validate(); err != nil {
		return err
	}

	// TODO(johnny): Enable, use in a proper E2E integration test.
	var res = consumer.Resolution{
		Header: pb.Header{ // Fake data just to validate.
			ProcessId: pb.ProcessSpec_ID{Zone: "local", Suffix: "peer"},
			Route:     pb.Route{},
			Etcd:      pb.Header_Etcd{},
		},
		Done: func() {},
	}
	var err error

	if false {
		res, err = api.resolver.Resolve(consumer.ResolveArgs{
			Context:     stream.Context(),
			ShardID:     req.Config.CoordinatorShard(),
			MayProxy:    false,
			ProxyHeader: req.Resolution,
		})
	}
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

	// TODO(johnny): Pluck *coordinator from resolved |res.Store|.
	var coordinator = api.fooCoordinator

	var ring = coordinator.findOrCreateRing(req.Config)
	var doneCh = make(chan error, 1)

	ring.subscriberCh <- subscriber{
		request: *req,
		sendMsg: stream.SendMsg,
		doneCh:  doneCh,
	}
	err = <-doneCh

	if stream.Context().Err() != nil {
		err = nil // Peer cancellations are not an error.
	} else if err != nil {
		// We got an error on SendMsg to the peer, which as-implemented by gRPC is always an EOF.
		err = stream.RecvMsg(new(pf.ShuffleRequest)) // Read a more descriptive error.

		log.WithFields(log.Fields{
			"err":       err,
			"journal":   req.Config.Journal,
			"ringName":  req.Config.Ring.Name,
			"ringIndex": req.RingIndex,
		}).Warn("failed to send ShuffleResponse to client")
	}
	return err
}
