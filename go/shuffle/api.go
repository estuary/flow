package shuffle

import (
	pf "github.com/estuary/flow/go/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// API is the server side implementation of the Shuffle protocol.
type API struct {
	resolver *consumer.Resolver
}

// Shuffle implements the gRPC Shuffle endpoint.
// TODO(johnny) This compiles, and is approximately right, but is untested and I'm
// none too sure of the details.
func (api *API) Shuffle(req *pf.ShuffleRequest, stream pf.Shuffler_ShuffleServer) error {
	if err := req.Validate(); err != nil {
		return err
	}
	var res, err = api.resolver.Resolve(consumer.ResolveArgs{
		Context:     stream.Context(),
		ShardID:     req.Config.CoordinatorShard(),
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

	// TODO(johnny): |res.Store| will host a single coordinator instance.
	var coordinator *coordinator

	var ring = coordinator.findOrCreateRing(res.Shard, req.Config)
	var doneCh = make(chan error, 1)

	ring.subscriberCh <- subscriber{
		request: *req,
		stream:  stream,
		doneCh:  doneCh,
	}
	return <-doneCh
}
