package network

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strconv"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/prometheus/client_golang/prometheus"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

// proxyClient is a connection between the frontend
// and the shard assignment currently hosting the connector.
type proxyClient struct {
	buf    []byte                      // Unread remainder of last response.
	picked pc.ListResponse_Shard       // Picked primary shard assignment and route.
	rpc    pf.NetworkProxy_ProxyClient // Running RPC.
	rxCh   chan struct{}               // Token for a capability to read from `rpc`.
	labels []string                    // Metric labels.
	nWrite prometheus.Counter          // Accumulates bytes written.
	nRead  prometheus.Counter          // Accumulates bytes read.
}

func dialShard(
	ctx context.Context,
	networkClient pf.NetworkProxyClient,
	shardClient pc.ShardClient,
	parsed parsedSNI,
	resolved resolvedSNI,
	userAddr string,
) (*proxyClient, error) {
	var labels = []string{resolved.taskName, parsed.port, resolved.portProtocol}
	shardStartedCounter.WithLabelValues(labels...).Inc()

	var fetched, err = listShards(ctx, shardClient, parsed, resolved.shardIDPrefix)
	if err == context.Canceled {
		shardHandledCounter.WithLabelValues(append(labels, "ListCancelled")...).Inc()
		return nil, err
	} else if err != nil {
		shardHandledCounter.WithLabelValues(append(labels, "ErrList")...).Inc()
		return nil, fmt.Errorf("failed to list matching task shards: %w", err)
	}

	// Pick a random primary.
	rand.Shuffle(len(fetched), func(i, j int) { fetched[i], fetched[j] = fetched[j], fetched[i] })

	var primary = -1
	for i := range fetched {
		if fetched[i].Route.Primary != -1 {
			primary = i
			break
		}
	}
	if primary == -1 {
		shardHandledCounter.WithLabelValues(append(labels, "ErrNoPrimary")...).Inc()
		return nil, fmt.Errorf("task has no ready primary shard assignment")
	}

	var claims = pb.Claims{
		Capability: pf.Capability_NETWORK_PROXY,
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet("id:prefix", resolved.shardIDPrefix),
		},
	}
	var picked = fetched[primary]

	rpc, err := networkClient.Proxy(
		// Build a context that routes to the shard primary and encodes `claims`.
		// We do not wrap `ctx` because that's only the context for dialing,
		// and not the context of the long-lived connection that results.
		pb.WithDispatchRoute(
			pb.WithClaims(context.Background(), claims),
			picked.Route,
			picked.Route.Members[picked.Route.Primary],
		),
	)
	if err != nil {
		shardHandledCounter.WithLabelValues(append(labels, "ErrCallProxy")...).Inc()
		return nil, fmt.Errorf("failed to start network proxy RPC to task shard: %w", err)
	}

	var port, _ = strconv.ParseUint(parsed.port, 10, 16) // parseSNI() already verified.
	var openErr = rpc.Send(&pf.TaskNetworkProxyRequest{
		Open: &pf.TaskNetworkProxyRequest_Open{
			ShardId:    picked.Spec.Id,
			TargetPort: uint32(port),
			ClientAddr: userAddr,
		},
	})

	opened, err := rpc.Recv()
	if err != nil {
		err = fmt.Errorf("failed to read opened response from task shard: %w", pf.UnwrapGRPCError(err))
	} else if opened.OpenResponse == nil {
		err = fmt.Errorf("task shard proxy RPC is missing expected OpenResponse")
	} else if status := opened.OpenResponse.Status; status != pf.TaskNetworkProxyResponse_OK {
		err = fmt.Errorf("task shard proxy RPC has non-ready status: %s", status)
	} else if openErr != nil {
		err = fmt.Errorf("failed to send open request: %w", err)
	}

	if err != nil {
		rpc.CloseSend()
		_, _ = rpc.Recv()
		shardHandledCounter.WithLabelValues(append(labels, "ErrOpen")...).Inc()
		return nil, err
	}

	var rxCh = make(chan struct{}, 1)
	rxCh <- struct{}{}

	// Received and sent from the user's perspective.
	var nWrite = bytesReceivedCounter.WithLabelValues(labels...)
	var nRead = bytesSentCounter.WithLabelValues(labels...)

	return &proxyClient{
		buf:    nil,
		picked: picked,
		rpc:    rpc,
		rxCh:   rxCh,
		labels: labels,
		nWrite: nWrite,
		nRead:  nRead,
	}, nil
}

// Write to the shard proxy client. MUST not be called concurrently with Close.
func (pc *proxyClient) Write(b []byte) (n int, err error) {
	if err = pc.rpc.Send(&pf.TaskNetworkProxyRequest{Data: b}); err != nil {
		return 0, err // This is io.EOF if the RPC is reset.
	}
	pc.nWrite.Add(float64(len(b)))
	return len(b), nil
}

// Read from the shard proxy client. MAY be called concurrently with Close.
func (pc *proxyClient) Read(b []byte) (n int, err error) {
	if len(pc.buf) == 0 {
		if _, ok := <-pc.rxCh; !ok {
			return 0, io.EOF // RPC already completed.
		}

		if rx, err := pc.rpc.Recv(); err != nil {
			close(pc.rxCh)

			if err == io.EOF {
				shardHandledCounter.WithLabelValues(append(pc.labels, "OK")...).Inc()
			} else {
				shardHandledCounter.WithLabelValues(append(pc.labels, "ErrRead")...).Inc()
			}
			return 0, pf.UnwrapGRPCError(err)
		} else {
			pc.buf = rx.Data
			pc.rxCh <- struct{}{} // Yield token.
			pc.nRead.Add(float64(len(rx.Data)))
		}
	}

	var i = copy(b, pc.buf)
	pc.buf = pc.buf[i:]
	return i, nil
}

// Close the proxy client. MAY be called concurrently with Read.
func (pc *proxyClient) Close() error {
	// Note that http.Transport in particular will sometimes but not always race
	// calls of Read() and Close(). We must ensure the RPC reads a final error as
	// part of Close(), because we can't guarantee a current or future call to
	// Read() will occur, but there may also be a raced Read() which will receive
	// EOF after we CloseSend() -- and if we naively attempted another pc.rpc.Recv()
	// it would block forever.
	var _ = pc.rpc.CloseSend()

	if _, ok := <-pc.rxCh; !ok {
		return nil // Read already completed.
	}
	close(pc.rxCh) // Future Read()'s return EOF.

	for {
		if _, err := pc.rpc.Recv(); err == io.EOF {
			shardHandledCounter.WithLabelValues(append(pc.labels, "OK")...).Inc()
			return nil
		} else if err != nil {
			shardHandledCounter.WithLabelValues(append(pc.labels, "ErrClose")...).Inc()
			return pf.UnwrapGRPCError(err)
		}
	}
}

func (sc *proxyClient) LocalAddr() net.Addr                { return nil }
func (sc *proxyClient) RemoteAddr() net.Addr               { return nil }
func (sc *proxyClient) SetDeadline(t time.Time) error      { return nil }
func (sc *proxyClient) SetReadDeadline(t time.Time) error  { return nil }
func (sc *proxyClient) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = &proxyClient{}
