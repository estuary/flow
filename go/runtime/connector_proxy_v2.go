package runtime

import (
	"io"

	pcn "github.com/estuary/flow/go/protocols/connector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// connectorProxyV2 exposes the reactor's `connector.Connector` service on its
// public address. It's a pure pass-through onto the singleton V2 task service
// hosted in this process: the caller's `authorization` header is forwarded
// verbatim, and the Rust `Authenticator` behind `conn` is the sole verifier
// (it checks `iss`, which Go's `KeyedAuth.Verify` does not).
type connectorProxyV2 struct{ conn *grpc.ClientConn }

func (p *connectorProxyV2) Connector(stream pcn.Connector_ConnectorServer) error {
	var md, _ = metadata.FromIncomingContext(stream.Context())
	var ctx = metadata.NewOutgoingContext(stream.Context(), md.Copy())

	if client, err := pcn.NewConnectorClient(p.conn).Connector(ctx); err != nil {
		return err
	} else {
		return runProxy(stream, client)
	}
}

// runProxy forwards a server stream onto a client stream in both directions,
// propagating the client's EOF or error back to the server.
func runProxy[
	Response any,
	Request any,
	ServerStream interface {
		Send(*Response) error
		Recv() (*Request, error)
	},
	ClientStream interface {
		Send(*Request) error
		Recv() (*Response, error)
		CloseSend() error
	},
](server ServerStream, client ClientStream) error {
	var fwdCh = make(chan error, 1)

	// Start a forwarding loop, which sends client messages into the proxied client.
	go func() (_err error) {
		defer func() { fwdCh <- _err }()

		for {
			if req, err := server.Recv(); err != nil {
				if err == io.EOF {
					return client.CloseSend() // Graceful EOF.
				} else {
					_ = client.CloseSend()
					return err
				}
			} else if err := client.Send(req); err != nil {
				return err
			}
		}
	}()

	// Run the reverse loop synchronously.
	for {
		if resp, err := client.Recv(); err != nil {
			if err == io.EOF {
				return <-fwdCh // Await and return an error from the forward loop.
			} else {
				return err
			}
		} else if err := server.Send(resp); err != nil {
			return err
		}
	}
}
