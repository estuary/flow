package runtime

import (
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
