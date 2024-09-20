package network

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/net/trace"
)

// ProxyServer is the "backend" of connector networking.
// It accepts Proxy requests from the Frontend and connects them to the
// corresponding TCP port of a running connector container.
type ProxyServer struct {
	Resolver *consumer.Resolver
}

func (ps *ProxyServer) Proxy(claims pb.Claims, stream pf.NetworkProxy_ProxyServer) (_err error) {
	var ctx = stream.Context()

	var open, err = stream.Recv()
	if err != nil {
		return err
	} else if err := validateOpen(open); err != nil {
		return fmt.Errorf("invalid open proxy message: %w", err)
	}

	resolution, err := ps.Resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		Claims:      claims,
		MayProxy:    false,
		ProxyHeader: open.Open.Header,
		ReadThrough: nil,
		ShardID:     open.Open.ShardId,
	})
	if err != nil {
		return err
	}

	var opened = &pf.TaskNetworkProxyResponse{
		OpenResponse: &pf.TaskNetworkProxyResponse_OpenResponse{
			Status: pf.TaskNetworkProxyResponse_Status(resolution.Status),
			Header: &resolution.Header,
		},
	}
	if resolution.Status != pc.Status_OK {
		return stream.Send(opened)
	}

	// Resolve the target port to the current container.
	var container, publisher = resolution.Store.(interface {
		ProxyHook() (*pr.Container, ops.Publisher)
	}).ProxyHook()
	resolution.Done()

	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("resolved container: %s", container.String())
	}

	if container == nil {
		// Container is not currently running.
		opened.OpenResponse.Status = pf.TaskNetworkProxyResponse_SHARD_STOPPED
		return stream.Send(opened)
	} else if open.Open.TargetPort == uint32(connectorInitPort) {
		opened.OpenResponse.Status = pf.TaskNetworkProxyResponse_PORT_NOT_ALLOWED
		return stream.Send(opened)
	}

	// Identify a proxy address to use from the container's published ports.
	var address string
	for _, port := range container.NetworkPorts {
		if open.Open.TargetPort != port.Number {
			continue
		}

		if m, ok := container.MappedHostPorts[open.Open.TargetPort]; ok {
			address = m
		} else {
			address = fmt.Sprintf("%s:%d", container.IpAddr, open.Open.TargetPort)
		}
		break
	}
	if address == "" {
		opened.OpenResponse.Status = pf.TaskNetworkProxyResponse_PORT_NOT_ALLOWED
		return stream.Send(opened)
	}

	// Dial the container.
	var dialer net.Dialer
	dialed, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to container: %w", err)
	}
	var delegate = dialed.(*net.TCPConn)
	defer delegate.Close()

	// All validations were successful and we dialed the container.
	_ = stream.Send(opened)

	ops.PublishLog(publisher, ops.Log_debug, "started TCP proxy connection to container",
		"clientAddr", open.Open.ClientAddr,
		"targetPort", open.Open.TargetPort,
	)
	var inbound, outbound uint64

	defer func() {
		ops.PublishLog(publisher, ops.Log_debug, "completed TCP proxy connection to container",
			"clientAddr", open.Open.ClientAddr,
			"targetPort", open.Open.TargetPort,
			"bytesIn", atomic.LoadUint64(&inbound),
			"byteOut", outbound,
			"error", _err,
		)
	}()

	// Forward loop that proxies from `client` => `delegate`.
	go func() {
		defer delegate.CloseWrite()

		for {
			if request, err := stream.Recv(); err != nil {
				err = pf.UnwrapGRPCError(err)

				if err != context.Canceled && err != io.EOF {
					ops.PublishLog(publisher, ops.Log_debug, "proxy client stream finished with error",
						"clientAddr", open.Open.ClientAddr,
						"error", err,
					)
				}
				return
			} else if n, err := delegate.Write(request.Data); err != nil {
				// Delegate reset its connection.
				// This is allowed, and we simply stop forwarding.
				// The RPC will return and the end client may then get a propagated RST.
				return
			} else {
				atomic.AddUint64(&inbound, uint64(n))
			}
		}
	}()

	// Backward loop that proxies from `delegate` => `client`.
	// When this loop completes, so does the Proxy RPC.

	var buffer = make([]byte, 1<<14) // 16KB.
	for {
		if n, err := delegate.Read(buffer); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("reading from container: %w", err)
		} else if err = stream.Send(&pf.TaskNetworkProxyResponse{Data: buffer[:n]}); err != nil {
			// `client` reset its connection. We logged a received client error
			// in the forwarding loop, and don't consider this reset to be an error.
			return nil
		} else {
			outbound += uint64(n)
		}
	}
}

func validateOpen(req *pf.TaskNetworkProxyRequest) error {
	if req.Open == nil {
		return fmt.Errorf("missing open message")
	}
	if err := req.Open.ShardId.Validate(); err != nil {
		return fmt.Errorf("invalid shard id: %w", err)
	}
	if req.Open.TargetPort == 0 {
		return fmt.Errorf("missing target port")
	}
	if req.Open.TargetPort > 65535 {
		return fmt.Errorf("target port '%d' out of range", req.Open.TargetPort)
	}
	if req.Open.ClientAddr == "" {
		return fmt.Errorf("missing client addr")
	}
	if len(req.Data) > 0 {
		return fmt.Errorf("first proxy message cannot have both Open and Data")
	}

	return nil
}

// See crates/runtime/src/container.rs
const connectorInitPort = 49092
