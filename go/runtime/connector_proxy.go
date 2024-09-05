package runtime

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/estuary/flow/go/bindings"
	pc "github.com/estuary/flow/go/protocols/capture"
	pd "github.com/estuary/flow/go/protocols/derive"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type connectorProxy struct {
	address   pb.Endpoint
	host      *FlowConsumer
	mu        sync.Mutex
	runtimes  map[string]*grpc.ClientConn
	semaphore chan struct{}
}

func (s *connectorProxy) ProxyConnectors(stream pr.ConnectorProxy_ProxyConnectorsServer) error {
	var _, cancel, _, err = s.host.Service.Verifier.Verify(stream.Context(), pf.Capability_PROXY_CONNECTOR)

	if err != nil {
		return err
	}
	defer cancel()

	// Use `semaphore` to constrain the total number of allowed proxy runtimes.
	s.semaphore <- struct{}{}
	defer func() { <-s.semaphore }()

	// Unique id for this proxy, that we'll pass back to the client.
	var id = fmt.Sprintf("connector-proxy-%d", time.Now().UnixNano())

	svc, err := bindings.NewTaskService(
		pr.TaskServiceConfig{
			AllowLocal:       s.host.Config.Flow.AllowLocal,
			ContainerNetwork: s.host.Config.Flow.Network,
			TaskName:         id,
			UdsPath:          path.Join(os.TempDir(), id),
		},
		func(log ops.Log) {
			_ = stream.Send(&pr.ConnectorProxyResponse{Log: &log})
		},
	)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.runtimes[id] = svc.Conn()
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.runtimes, id)
		s.mu.Unlock()

		svc.Drop()
	}()

	// Now that we've indexed `id`, tell the client about it.
	_ = stream.Send(&pr.ConnectorProxyResponse{
		Address: s.address,
		ProxyId: id,
	})

	// Block until we read EOF, signaling a graceful shutdown.
	for {
		if _, err = stream.Recv(); err == nil {
			continue
		}
		if err == io.EOF {
			err = nil
		} else {
			logrus.WithFields(logrus.Fields{"err": err, "id": id}).
				Warn("unclean shutdown of connector proxy runtime")
		}
		return err
	}
}

func (s *connectorProxy) Capture(stream pc.Connector_CaptureServer) error {
	var ctx, cancel, conn, err = s.verify(stream.Context())
	if err != nil {
		return err
	}
	defer cancel()

	if proxy, err := pc.NewConnectorClient(conn).Capture(ctx); err != nil {
		return err
	} else {
		return runProxy(stream, proxy, new(pc.Request), new(pc.Response))
	}
}

func (s *connectorProxy) Derive(stream pd.Connector_DeriveServer) error {
	var ctx, cancel, conn, err = s.verify(stream.Context())
	if err != nil {
		return err
	}
	defer cancel()

	if proxy, err := pd.NewConnectorClient(conn).Derive(ctx); err != nil {
		return err
	} else {
		return runProxy(stream, proxy, new(pd.Request), new(pd.Response))
	}
}

func (s *connectorProxy) Materialize(stream pm.Connector_MaterializeServer) error {
	var ctx, cancel, conn, err = s.verify(stream.Context())
	if err != nil {
		return err
	}
	defer cancel()

	if proxy, err := pm.NewConnectorClient(conn).Materialize(ctx); err != nil {
		return err
	} else {
		return runProxy(stream, proxy, new(pm.Request), new(pm.Response))
	}
}

func (s *connectorProxy) verify(ctx context.Context) (context.Context, context.CancelFunc, *grpc.ClientConn, error) {
	ctx, cancel, _, err := s.host.Service.Verifier.Verify(ctx, pf.Capability_PROXY_CONNECTOR)
	if err != nil {
		return nil, nil, nil, err
	}

	var md, _ = metadata.FromIncomingContext(ctx)
	if h := md.Get("proxy-id"); len(h) == 0 {
		cancel()
		return nil, nil, nil, fmt.Errorf("missing proxy-id header")
	} else {
		var id = h[0]

		s.mu.Lock()
		var conn, ok = s.runtimes[id]
		s.mu.Unlock()

		if !ok {
			cancel()
			return nil, nil, nil, fmt.Errorf("proxy-id %s not found", id)
		}
		return ctx, cancel, conn, nil
	}
}

func runProxy(server grpc.ServerStream, client grpc.ClientStream, req, resp any) error {
	var fwdCh = make(chan error, 1)

	// Start a forwarding loop, which sends client messages into the proxied client.
	go func() (_err error) {
		defer func() { fwdCh <- _err }()

		for {
			if err := server.RecvMsg(req); err != nil {
				if err == io.EOF {
					return client.CloseSend() // Graceful EOF.
				} else {
					_ = client.CloseSend()
					return err
				}
			}
			if err := client.SendMsg(req); err != nil {
				return err
			}
		}
	}()

	// Run the reverse loop synchronously.
	for {
		if err := client.RecvMsg(resp); err != nil {
			if err == io.EOF {
				return <-fwdCh // Await and return an error from the forward loop.
			} else {
				return err
			}
		}
		if err := server.SendMsg(resp); err != nil {
			return err
		}
	}
}

var _ pr.ConnectorProxyServer = &connectorProxy{}
var _ pc.ConnectorServer = &connectorProxy{}
var _ pd.ConnectorServer = &connectorProxy{}
var _ pm.ConnectorServer = &connectorProxy{}
