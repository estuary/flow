package runtime

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/protocols/capture"
	pf "github.com/estuary/protocols/flow"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/protocol"
	"google.golang.org/grpc"
)

// TODO(johnny): This implementation builds and I believe it to be correct
// and what we want, but it's also untested at this time (November 2021).
// I'm putting it down for the time being, with plans to pick it up as we
// re-add features around HTTP, WebSocket, and Kafka push ingestion.
var _ capture.RuntimeServer = (*FlowConsumer)(nil)

func (f *FlowConsumer) Push(stream capture.Runtime_PushServer) error {
	// Read and validate the Push Open.
	req, err := stream.Recv()
	if err != nil {
		return err
	} else if err = req.Validate(); err != nil {
		return err
	} else if req.Open == nil {
		return fmt.Errorf("expected Open")
	}
	var open = req.Open

	// List shards of the capture.
	list, err := f.Service.List(stream.Context(), &protocol.ListRequest{
		Selector: pf.LabelSelector{
			Include: pb.MustLabelSet(
				labels.TaskName, open.Capture.String(),
				labels.TaskType, labels.TaskTypeCapture,
			)},
	})
	if err != nil {
		return err
	} else if len(list.Shards) == 0 {
		return fmt.Errorf("capture %s not found", open.Capture)
	}

	// Select a shard to which this RPC is routed.
	var index = rand.Intn(len(list.Shards))

	// If we received a proxy Header, prefer a shard having the header
	// process ID as primary (we'll verify the header during Resolve).
	if open.Header != nil {
		for i, shard := range list.Shards {
			if shard.Route.Primary != -1 && shard.Route.Members[shard.Route.Primary] == open.Header.ProcessId {
				index = i
				break
			}
		}
	}
	var shard = list.Shards[index]

	// Resolve the shard to a serving process.
	res, err := f.Service.Resolver.Resolve(consumer.ResolveArgs{
		Context:     stream.Context(),
		ShardID:     shard.Spec.Id,
		MayProxy:    true,
		ProxyHeader: open.Header,
	})
	if err != nil {
		return fmt.Errorf("resolving shard %s: %w", shard.Spec.Id, err)
	} else if res.Status != protocol.Status_OK {
		return fmt.Errorf(res.Status.String())
	}

	// If the shard isn't local, proxy RPC to the resolved primary peer.
	if res.Store == nil {
		req.Open.Header = &res.Header
		return proxyPush(stream, *req, capture.NewRuntimeClient(f.Service.Loopback))
	}
	defer res.Done()

	var push, ok = res.Store.(*Capture).delegate.(*capture.PushServer)
	if !ok {
		return fmt.Errorf("capture %s is not an ingestion", open.Capture)
	}

	var ackCh = make(chan struct{}, 128)
	var readCh = make(chan error, 1)
	var started, finished int

	go func() (__out error) {
		defer func() { readCh <- __out }()

		for {
			var docs, checkpoint, err = capture.ReadPushCheckpoint(stream, maxPushByteSize)
			if err != nil {
				return fmt.Errorf("reading push checkpoint: %w", err)
			} else if err = push.Push(docs, checkpoint, ackCh); err != nil {
				return fmt.Errorf("staging push for capture: %w", err)
			}
			started++
		}
	}()

	for {
		select {
		case <-push.ServeOp().Done():
			return push.ServeOp().Err()

		case err := <-readCh:
			if err != io.EOF {
				return err
			}
			readCh = nil // Graceful drain. Don't select again.

		case <-ackCh:
			if err = stream.Send(&capture.PushResponse{
				Acknowledge: &capture.Acknowledge{},
			}); err != nil {
				return err
			}

			finished++

			// Termination condition: exit if we've read EOF and
			// received all expected ACK's.
			if readCh == nil && started == finished {
				return nil
			}
		}
	}
}

// proxyPush forwards a PushRequest to a resolved peer.
// Pass request by value as we'll later mutate it (via RecvMsg).
func proxyPush(stream grpc.ServerStream, req capture.PushRequest, client capture.RuntimeClient) error {
	var ctx = pb.WithDispatchRoute(stream.Context(), req.Open.Header.Route, req.Open.Header.ProcessId)

	var rpc, err = client.Push(ctx)
	if err != nil {
		return err
	}

	// Loop that reads from |stream| and sends to |rpc|.
	go func() {
		defer rpc.CloseSend()

		for {
			if err = rpc.SendMsg(&req); err != nil {
				return // Client stream is broken. RecvMsg() will return causal error.
			} else if err = stream.RecvMsg(&req); err != nil {
				if err != io.EOF {
					logrus.WithError(err).Warn("failed to proxy Push RPC")
				}
				return
			}
		}
	}()

	// Loop to read from |rpc| and send to |stream|.
	var resp = new(capture.PushResponse)
	for {
		if err = rpc.RecvMsg(resp); err != nil {
			return err // Proxy to caller.
		} else if err = stream.SendMsg(resp); err != nil {
			return err // Client broke the transport.
		}
	}
}

const maxPushByteSize = (1 << 21) // 2MB.
