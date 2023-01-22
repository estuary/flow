package runtime

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc"
)

type ProxyServer struct {
	mu         *sync.Mutex
	containers map[pc.ShardID]*runningContainer
	resolver   *consumer.Resolver
}

type runningContainer struct {
	ports      map[string]bool
	connection *grpc.ClientConn
	logger     ops.Publisher
}

func NewProxyServer(resolver *consumer.Resolver) *ProxyServer {
	return &ProxyServer{
		mu:         &sync.Mutex{},
		containers: make(map[pc.ShardID]*runningContainer),
		resolver:   resolver,
	}
}

// TODO: log unsuccessful proxy connnections
// TODO: publish stats
func (ps *ProxyServer) Proxy(streaming pf.NetworkProxy_ProxyServer) error {
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
			"portName":            open.PortName,
			"hasRunningContainer": container != nil,

			// TODO: better message
		}).Warn("cannot open proxy connection")
		return streaming.Send(&pf.TaskNetworkProxyResponse{
			OpenResponse: openResp,
		})
	}

	// In the happy path, we'll wait to send the response until after we've gotten
	// a response from the container. This is to make it more obvious when there
	// are problems with that connection, and so we can return the Status from the
	// container's proxy service if it's not OK.
	var proxyContext, cancelFunc = context.WithCancel(streaming.Context())
	defer cancelFunc()
	logrus.WithFields(logrus.Fields{
		"shardID":  open.ShardId,
		"portName": open.PortName,
		"clientIP": open.ClientIp,
	}).Info("network proxy request is valid and container is running, starting handshake")
	var client = pf.NewNetworkProxyClient(container.connection)
	proxyClient, err := client.Proxy(proxyContext)
	if err != nil {
		return fmt.Errorf("dialing client: %w", err)
	}
	var handshakeComplete = false
	defer func() {
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
			"portName": open.PortName,
		}).Warn("connector-init returned !OK")
		openResp.Status = proxyOpenResp.OpenResponse.Status
		return streaming.Send(&pf.TaskNetworkProxyResponse{
			OpenResponse: openResp,
		})
	}

	if err = streaming.Send(&pf.TaskNetworkProxyResponse{OpenResponse: openResp}); err != nil {
		return fmt.Errorf("sending open response: %w", err)
	}

	ops.PublishLog(container.logger, pf.LogLevel_debug, "proxy connection opened", "port", open.PortName, "clientIP", open.ClientIp)

	go func() {
		if e := copyResponses(proxyContext, streaming, proxyClient); isFailure(e) {
			ops.PublishLog(container.logger, pf.LogLevel_warn, "proxy response stream failed", "port", open.PortName, "clientIP", open.ClientIp, "error", e)
			// TODO: take another look at how these contexts and cancellation are handled
			cancelFunc()
		}
	}()

	if err = copyRequests(streaming, proxyClient); isFailure(err) {
		ops.PublishLog(container.logger, pf.LogLevel_warn, "proxy request stream failed", "port", open.PortName, "clientIP", open.ClientIp, "error", err)
		cancelFunc()
	}
	cancelFunc()
	return err
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
	if _, ok := container.ports[open.PortName]; !ok {
		resp.Status = pf.TaskNetworkProxyResponse_PORT_NOT_ALLOWED
		return resp, nil, nil
	}

	return resp, container, nil
}

//func (ps. *ProxyServer)

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
		//ReadThrough: nil, because we don't care about the progress of the shard, only whether this instance is primary
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
	return err != io.EOF && err != context.Canceled
}

//func (ps *ProxyServer) tryStartProxy(streaming pf.NetworkProxy_ProxyServer) (*shard)

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
	if req.Open.PortName == "" {
		return fmt.Errorf("missing port name")
	}
	if req.Open.ClientIp == "" {
		return fmt.Errorf("missing client ip")
	}
	if len(req.Data) > 0 {
		return fmt.Errorf("first proxy message cannot have both Open and Data")
	}

	return nil
}

func copyResponses(ctx context.Context, streaming pf.NetworkProxy_ProxyServer, client pf.NetworkProxy_ProxyClient) error {
	for ctx.Err() == nil {
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
	}
	return ctx.Err()
}

func copyRequests(streaming pf.NetworkProxy_ProxyServer, client pf.NetworkProxy_ProxyClient) error {
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
	}
}

func (ps *ProxyServer) ContainerStarted(shardID pc.ShardID, grpcConn *grpc.ClientConn, logger ops.Publisher, ports []string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	logrus.WithFields(logrus.Fields{
		"shardID": shardID,
		"ports":   strings.Join(ports, ","),
	}).Info("enabling proxy connections for container")

	var portSet = make(map[string]bool)
	for _, port := range ports {
		portSet[port] = true
	}
	ps.containers[shardID] = &runningContainer{
		ports:      portSet,
		connection: grpcConn,
		logger:     logger,
	}
}

func (ps *ProxyServer) ContainerStopped(shardID pc.ShardID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.containers, shardID)
}
