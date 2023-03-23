package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	po "github.com/estuary/flow/go/protocols/ops"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type ProxyServer struct {
	mu         sync.Mutex
	containers map[pc.ShardID]*runningContainer
	resolver   *consumer.Resolver
}

func NewProxyServer(resolver *consumer.Resolver) *ProxyServer {
	return &ProxyServer{
		containers: make(map[pc.ShardID]*runningContainer),
		resolver:   resolver,
	}
}

// NetworkConfigHandle returns a handle that can be passed to `connector.StartContainer` to expose the given set of ports when
// the container is started, and stop exposing them once the container is stopped.
func (ps *ProxyServer) NetworkConfigHandle(shardID pc.ShardID, ports map[uint16]*labels.PortConfig) connector.ExposePorts {
	return &networkConfigHandle{
		server:  ps,
		shardID: shardID,
		ports:   ports,
	}
}

func (ps *ProxyServer) Proxy(streaming pf.NetworkProxy_ProxyServer) error {
	var handshakeComplete = false
	defer func() {
		if !handshakeComplete {
			proxyConnectionRejectedCounter.Inc()
		}
	}()
	var req, err = streaming.Recv()
	if err != nil {
		return err
	}

	if err = validateOpen(req); err != nil {
		return fmt.Errorf("invalid open proxy message: %w", err)
	}
	var open = req.Open

	openResp, container, err := ps.openConnection(streaming.Context(), open)
	if err != nil {
		return err
	}
	if openResp.Status != pf.TaskNetworkProxyResponse_OK {
		logrus.WithFields(logrus.Fields{
			"status":              openResp.Status,
			"shardID":             open.ShardId,
			"portName":            open.TargetPort,
			"hasRunningContainer": container != nil,
		}).Warn("network connection rejected by connector")
		return streaming.Send(&pf.TaskNetworkProxyResponse{
			OpenResponse: openResp,
		})
	}

	var inboundBytes, outboundBytes uint64

	ops.PublishLog(container.logger, po.Log_info, "opening inbound connection to container",
		"clientAddr", open.ClientAddr,
		"targetPort", open.TargetPort,
	)

	defer func() {
		var logErr = err
		if errors.Is(logErr, io.EOF) {
			logErr = nil
		}
		ops.PublishLog(container.logger, po.Log_info, "proxy connection closed",
			"clientAddr", open.ClientAddr,
			"targetPort", open.TargetPort,
			"inboundBytes", inboundBytes,
			"outboundBytes", outboundBytes,
			"error", logErr,
		)
	}()

	// In the happy path, we'll wait to send the response until after we've gotten
	// a response from the container. This is to make it more obvious when there
	// are problems with that connection, and so we can return the Status from the
	// container's proxy service if it's not OK.
	var proxyContext, cancelFunc = context.WithCancel(streaming.Context())
	defer cancelFunc()
	logrus.WithFields(logrus.Fields{
		"shardID":    open.ShardId,
		"targetPort": open.TargetPort,
		"clientAddr": open.ClientAddr,
	}).Info("network proxy request is valid and container is running, starting handshake")
	var client = pf.NewNetworkProxyClient(container.connection)
	proxyClient, err := client.Proxy(proxyContext)
	if err != nil {
		return fmt.Errorf("dialing client: %w", err)
	}
	defer func() {
		// only close send if we have not actually completed the handhshake.
		// If we did complete the handshake, the CloseSend will be called within
		// copyRequests.
		if !handshakeComplete {
			proxyClient.CloseSend()
		}
	}()
	if err = proxyClient.Send(&pf.TaskNetworkProxyRequest{
		Open: open,
	}); err != nil {
		return fmt.Errorf("sending open req to container: %w", err)
	}
	proxyOpenResp, err := proxyClient.Recv()
	if err != nil {
		return fmt.Errorf("receiving open resp from container: %w", err)
	}
	if proxyOpenResp.OpenResponse == nil {
		return fmt.Errorf("internal protocol error, expected OpenResponse from container")
	}
	if proxyOpenResp.OpenResponse.Status != pf.TaskNetworkProxyResponse_OK {
		logrus.WithFields(logrus.Fields{
			"status":   proxyOpenResp.OpenResponse.Status,
			"shardID":  open.ShardId,
			"portName": open.TargetPort,
		}).Warn("connector-init returned !OK")
		openResp.Status = proxyOpenResp.OpenResponse.Status
		return streaming.Send(&pf.TaskNetworkProxyResponse{
			OpenResponse: openResp,
		})
	}

	if err = streaming.Send(&pf.TaskNetworkProxyResponse{OpenResponse: openResp}); err != nil {
		return fmt.Errorf("sending open response: %w", err)
	}

	handshakeComplete = true
	var shardID = open.ShardId.String()
	proxyConnectionsAcceptedCounter.WithLabelValues(shardID, strconv.Itoa(int(open.TargetPort))).Inc()
	ops.PublishLog(container.logger, po.Log_debug, "proxy connection opened", "port", open.TargetPort, "clientIP", open.ClientAddr)

	var grp = errgroup.Group{}

	grp.Go(func() error {
		if e := copyResponses(streaming, proxyClient, shardID, open.TargetPort, &outboundBytes); isFailure(e) {
			return fmt.Errorf("copying outbound data: %w", e)
		}
		return nil
	})
	grp.Go(func() error {
		if e := copyRequests(streaming, proxyClient, shardID, open.TargetPort, &inboundBytes); isFailure(e) {
			return fmt.Errorf("copying inbound data: %w", e)
		}
		return nil
	})

	err = grp.Wait()
	var status = "ok"
	if err != nil {
		status = "error"
		ops.PublishLog(container.logger, po.Log_warn, "proxy connection failed", "port", open.TargetPort, "clientAddr", open.ClientAddr, "error", err)
	} else {
		ops.PublishLog(container.logger, po.Log_debug, "proxy connection closing normally", "port", open.TargetPort, "clientAddr", open.ClientAddr)
	}
	proxyConnectionsClosedCounter.WithLabelValues(shardID, strconv.Itoa(int(open.TargetPort)), status).Inc()
	return nil
}

func (ps *ProxyServer) openConnection(ctx context.Context, open *pf.TaskNetworkProxyRequest_Open) (*pf.TaskNetworkProxyResponse_OpenResponse, *runningContainer, error) {
	shardStatus, header, err := ps.checkShardStatus(ctx, open)
	if err != nil {
		return nil, nil, fmt.Errorf("resolving shard status: %w", err)
	}
	var resp = &pf.TaskNetworkProxyResponse_OpenResponse{
		Status: pf.TaskNetworkProxyResponse_Status(shardStatus),
		Header: &header,
	}
	if resp.Status != pf.TaskNetworkProxyResponse_OK {
		return resp, nil, nil
	}

	var container = ps.lookupContainer(open.ShardId)
	if container == nil {
		resp.Status = pf.TaskNetworkProxyResponse_SHARD_STOPPED
		return resp, nil, nil
	}
	// Disallow connections from the outside world to connector-init.
	if uint16(open.TargetPort) == connector.CONNECTOR_INIT_PORT {
		resp.Status = pf.TaskNetworkProxyResponse_PORT_NOT_ALLOWED
		return resp, container, nil
	}
	if _, isExposed := container.ports[uint16(open.TargetPort)]; !isExposed {
		resp.Status = pf.TaskNetworkProxyResponse_PORT_NOT_ALLOWED
		return resp, container, nil
	}

	return resp, container, nil
}

// checkShardStatus queries the `consumer.Resolver` to determine the current
// shard status and header. This may block in the case that this instance is
// Primary but the shard is still executing `RestoreCheckpoint`. It uses a
// timeout to ensure that we don't block indefinitely in case the shard blocks
// during `RestoreCheckpoint`. Note that capture containers do not get started
// as part of `RestoreCheckpoint`, and thus we should not expect capture
// containers to be started after a call to `checkShardStatus` finishes.
// Materialization containers do get started as part of `RestoreCheckpoint`,
// so we can expect to be able to proxy to if `Resolve` says we're primary. In
// most common cases, this function won't actually block (except as needed to
// acquire some mutex locks), so it should be pretty quick if another shard is
// primary or if the shard has finished starting up.
func (ps *ProxyServer) checkShardStatus(ctx context.Context, open *pf.TaskNetworkProxyRequest_Open) (status pc.Status, header pb.Header, err error) {
	ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	resolution, err := ps.resolver.Resolve(consumer.ResolveArgs{
		Context:     ctx,
		ShardID:     open.ShardId,
		MayProxy:    false,
		ProxyHeader: open.Header,
		// We're using the zero-value of `ReadThrough`, because we don't care about the progress of the shard, only whether this instance is primary
	})
	if err != nil {
		return
	}
	header = resolution.Header
	status = resolution.Status
	if resolution.Done != nil {
		resolution.Done()
	}
	return
}

func isFailure(err error) bool {
	return !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled)
}

func (ps *ProxyServer) lookupContainer(id pc.ShardID) *runningContainer {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.containers[id]
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

func copyResponses(streaming pf.NetworkProxy_ProxyServer, client pf.NetworkProxy_ProxyClient, shard string, port uint32, outboundBytes *uint64) error {
	var counter = proxyConnBytesOutboundCounter.WithLabelValues(shard, strconv.Itoa(int(port)))
	for {
		var resp, err = client.Recv()
		if err != nil {
			return err
		}

		// Validate the response by ensuring it has only Data
		if resp.OpenResponse != nil {
			return fmt.Errorf("data response contained OpenResponse")
		}
		if len(resp.Data) == 0 {
			return fmt.Errorf("response did not contain data")
		}
		if err = streaming.Send(resp); err != nil {
			return err
		}
		counter.Add(float64(len(resp.Data)))
		atomic.AddUint64(outboundBytes, uint64(len(resp.Data)))
	}
}

func copyRequests(streaming pf.NetworkProxy_ProxyServer, client pf.NetworkProxy_ProxyClient, shard string, port uint32, inboundBytes *uint64) error {
	var counter = proxyConnBytesInboundCounter.WithLabelValues(shard, strconv.Itoa(int(port)))
	defer client.CloseSend()
	for {
		var req, err = streaming.Recv()
		if err != nil {
			return err
		}
		// Validate the request by ensuring it has only Data
		if req.Open != nil {
			return fmt.Errorf("data message contained Open")
		}
		if len(req.Data) == 0 {
			return fmt.Errorf("request did not contain data")
		}
		if err = client.Send(req); err != nil {
			return err
		}
		counter.Add(float64(len(req.Data)))
		atomic.AddUint64(inboundBytes, uint64(len(req.Data)))
	}
}

type runningContainer struct {
	instanceVersion int
	ports           map[uint16]*labels.PortConfig
	connection      *grpc.ClientConn
	logger          ops.Publisher
}

type networkConfigHandle struct {
	server *ProxyServer
	// container lifecycles aren't always as straight forward as you'd hope for.
	// It can happen that a container exits, and `Unexpose` is called, before
	// `Expose` is called. This incrementing number is used to ensure correctness
	// in scenarios where things may be called out of order.
	instanceVersion int
	shardID         pc.ShardID
	ports           map[uint16]*labels.PortConfig
}

func (h *networkConfigHandle) Expose(connection *grpc.ClientConn, logger ops.Publisher) {
	h.server.mu.Lock()
	defer h.server.mu.Unlock()

	if h.instanceVersion > 0 {
		logrus.WithFields(logrus.Fields{
			"shardID": h.shardID,
		}).Info("ignoring raced call to expose ports")
		return
	}

	logrus.WithFields(logrus.Fields{
		"shardID": h.shardID,
		"ports":   h.ports,
	}).Info("enabling proxy connections for container")

	// If this call to Expose came prior to the call to Unexpose the previous instances' ports,
	// then increment the instanceVersion so that a delayed call to Unexpose can be ignored.
	if previousInstance := h.server.containers[h.shardID]; previousInstance != nil {
		h.instanceVersion = previousInstance.instanceVersion + 1
	} else {
		h.instanceVersion = 1
	}

	h.server.containers[h.shardID] = &runningContainer{
		instanceVersion: h.instanceVersion,
		ports:           h.ports,
		connection:      connection,
		logger:          logger,
	}

}

func (h *networkConfigHandle) Unexpose() {
	h.server.mu.Lock()
	defer h.server.mu.Unlock()

	if h.instanceVersion == 0 {
		// Expose was never called. This is normal if the container failed quickly.
		// Increment the instanceVersion so that a raced call to Expose can be ignored.
		h.instanceVersion = 1
		return
	}
	if previousInstance := h.server.containers[h.shardID]; previousInstance != nil {
		// Has Expose already been called again prior to this call to Unexpose?
		if previousInstance.instanceVersion > h.instanceVersion {
			return
		}
	}
	delete(h.server.containers, h.shardID)

	logrus.WithFields(logrus.Fields{
		"shardID": h.shardID,
	}).Debug("disabled proxy connections after container shutdown")
}

// Prometheus metrics for connector TCP proxying. The labels here will likely result
// in more timeseries than we'd like in the long term. But in the short term, I'm
// thinking it's better to have them to aid in debugging. These metrics match those
// collected by data-plane-gateway
var proxyConnectionsAcceptedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "net_proxy_conns_accept_total",
	Help: "counter of proxy connections that have been accepted",
}, []string{"shard", "port"})
var proxyConnectionsClosedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "net_proxy_conns_closed_total",
	Help: "counter of proxy connections that have completed and closed",
}, []string{"shard", "port", "status"})

var proxyConnectionRejectedCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "net_proxy_conns_reject_total",
	Help: "counter of proxy connections that have been rejected due to error or invalid sni",
})

var proxyConnBytesInboundCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "net_proxy_conn_inbound_bytes_total",
	Help: "total bytes proxied from client to container",
}, []string{"shard", "port"})
var proxyConnBytesOutboundCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "net_proxy_conn_outbound_bytes_total",
	Help: "total bytes proxied from container to client",
}, []string{"shard", "port"})
