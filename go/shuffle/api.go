package shuffle

import (
	"context"
	"io"

	pr "github.com/estuary/flow/go/protocols/runtime"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API is the server side implementation of the Shuffle protocol.
type API struct {
	// resolve is a consumer.Resolver.Resolve() closure, stubbed for easier testing.
	resolve resolveFn
}

type resolveFn func(args consumer.ResolveArgs) (consumer.Resolution, error)

// Store are interface expectations of a consumer.Store which is used
// by the shuffle subsystem.
type Store interface {
	// Coordinator returns the shared *Coordinator of this store.
	Coordinator() *Coordinator
}

// NewAPI returns a new *API using the given Resolver.
func NewAPI(resolver *consumer.Resolver) *API {
	return &API{resolve: resolver.Resolve}
}

// Shuffle implements the gRPC Shuffle endpoint.
func (api *API) Shuffle(claims pb.Claims, req *pr.ShuffleRequest, stream pr.Shuffler_ShuffleServer) error {
	if err := req.Validate(); err != nil {
		return err
	}
	var res, err = api.resolve(consumer.ResolveArgs{
		Context:     stream.Context(),
		Claims:      claims,
		ShardID:     req.Coordinator,
		MayProxy:    false,
		ProxyHeader: req.Resolution,
	})
	var resp = pr.ShuffleResponse{
		Status: res.Status,
		Header: &res.Header,
	}

	if err != nil {
		return err
	} else if resp.Status != pc.Status_OK {
		return stream.SendMsg(&resp)
	}
	defer res.Done()

	var coordinator = res.Store.(Store).Coordinator()
	var errCh = make(chan error, 1)

	// Begin a subscription that's delivered to the callback closure.
	coordinator.Subscribe(
		stream.Context(),
		*req,
		func(m *pr.ShuffleResponse, err error) error {
			if err != nil {
				errCh <- err
				close(errCh)
			} else if err = stream.Send(m); err == io.EOF {
				// EOF means the stream is broken; we can read a more descriptive error.
				err = stream.RecvMsg(new(pr.ShuffleRequest))
			}
			return err
		},
	)
	// Block until a final error is delivered.
	err = <-errCh

	if err == io.EOF || stream.Context().Err() != nil {
		err = nil // Not an error.
	} else if err == context.Canceled {
		// Map semantics to gRPC "Unavailable" status.
		err = status.Error(codes.Unavailable, "server cancelled")
	} else if err != nil {
		log.WithFields(log.Fields{
			"err":     err,
			"journal": req.Journal,
			"range":   req.Range,
		}).Warn("failed to serve Shuffle API")
	}
	return err
}
