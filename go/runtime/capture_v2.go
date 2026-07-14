package runtime

import (
	"fmt"
	"io"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/estuary/flow/go/shuffle"
	"github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// captureAppV2 is the runtime-next backed capture Application. Like the
// materialization V2 controller, it bypasses Gazette's transaction callbacks:
// Rust owns connector polling, document publishing, stats, and local RocksDB
// recovery-log persistence for the shard.
type captureAppV2 struct {
	*taskBase[*pf.CaptureSpec]

	client pr.Shard_CaptureClient
	respCh <-chan captureRecvResult
}

// captureRecvResult is one outcome of m.client.Recv(): either a Capture
// response or a terminal error (io.EOF on graceful close).
type captureRecvResult struct {
	resp *pr.Capture
	err  error
}

var _ application = (*captureAppV2)(nil)

func newCaptureAppV2(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*captureAppV2, error) {
	var base, err = newTaskBaseV2[*pf.CaptureSpec](host, shard, recorder, extractCaptureSpec)
	if err != nil {
		return nil, err
	}
	go base.heartbeatLoop(shard)

	var client pr.Shard_CaptureClient
	client, err = pr.NewShardClient(base.svc.Conn()).Capture(shard.Context())
	if err != nil {
		base.drop()
		return nil, fmt.Errorf("opening V2 Shard.Capture stream: %w", err)
	}

	var rocksDBDescriptor *pr.RocksDBDescriptor
	if recorder != nil {
		rocksDBDescriptor = bindings.NewRocksDBDescriptor(recorder)
	}
	_ = client.Send(&pr.Capture{
		SessionLoop: &pr.SessionLoop{
			RocksdbDescriptor: rocksDBDescriptor,
		},
	})

	var respCh = make(chan captureRecvResult, 4)
	go func() {
		defer close(respCh)
		for {
			var resp, err = client.Recv()
			// Bias towards send, but bail if full AND cancelled.
			select {
			case respCh <- captureRecvResult{resp: resp, err: err}:
			default:
				select {
				case respCh <- captureRecvResult{resp: resp, err: err}:
				case <-shard.Context().Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	return &captureAppV2{
		taskBase: base,
		client:   client,
		respCh:   respCh,
	}, nil
}

func (c *captureAppV2) recv() (*pr.Capture, error) {
	var r, ok = <-c.respCh
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return r.resp, r.err
}

func (c *captureAppV2) RestoreCheckpoint(shard consumer.Shard) (pf.Checkpoint, error) {
	if err := c.initTerm(shard); err != nil {
		c.term.cancel()
		return pf.Checkpoint{}, err
	}
	if !useRuntimeV2(shard.Spec().LabelSet) {
		c.term.cancel()
		return pf.Checkpoint{}, fmt.Errorf(
			"runtime-v2 feature flag is unset but this shard is running the V2 capture runtime; failing to force a restart")
	}
	return pf.Checkpoint{}, nil
}

func (c *captureAppV2) StartReadingMessages(shard consumer.Shard, _ pc.Checkpoint, _ *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {
	go c.runOneSession(shard, ch)
}

func (c *captureAppV2) runOneSession(shard consumer.Shard, ch chan<- consumer.EnvelopeOrError) (err error) {
	defer func() {
		if err != nil {
			ch <- consumer.EnvelopeOrError{Error: err}
		}
		close(ch)
	}()

	var specBytes []byte
	if specBytes, err = c.term.taskSpec.Marshal(); err != nil {
		return fmt.Errorf("marshaling CaptureSpec: %w", err)
	}

	// Build Join from this current shard topology, and send.
	var join *pr.Join
	if join, err = c.buildJoin(); err != nil {
		return fmt.Errorf("building Join: %w", err)
	}
	_ = c.client.Send(&pr.Capture{Join: join})

	// Receive Joined and expect trivial consensus.
	var resp *pr.Capture
	if resp, err = c.recv(); err != nil {
		return fmt.Errorf("receiving Joined: %w", pf.UnwrapGRPCError(err))
	} else if resp.Joined == nil {
		return fmt.Errorf("expected Joined, got %#v", resp)
	} else if resp.Joined.MaxEtcdRevision != 0 {
		return fmt.Errorf("Joined unexpectedly requested Etcd revision %d", resp.Joined.MaxEtcdRevision)
	}

	// Send task.
	_ = c.client.Send(&pr.Capture{
		Task: &pr.Task{
			Spec:            specBytes,
			MaxTransactions: 0,
		},
	})

	// Receive Opened.
	if resp, err = c.recv(); err != nil {
		return fmt.Errorf("receiving Opened: %w", pf.UnwrapGRPCError(err))
	} else if resp.Opened == nil {
		return fmt.Errorf("expected Opened, got %#v", resp)
	}
	c.container.Store(resp.Opened.Container)

	// Steady-state: drive teardown signals and surface stream errors.
	// termDone is nil-ed once we've sent Stop, so the case stops firing.
	// Future CloseNow plumbing slots in as another case alongside termDone.
	var termDone = c.term.ctx.Done()
	for {
		select {
		case <-termDone:
			// Spec update: initiate graceful drain. The leader replies with
			// Stopped, read from `respCh` below.
			_ = c.client.Send(&pr.Capture{Stop: &pr.Stop{}})
			termDone = nil

		case <-shard.Context().Done():
			return shard.Context().Err() // Immediate, non-graceful shutdown.

		case r, ok := <-c.respCh:
			if !ok || r.err == io.EOF {
				return nil
			}
			if r.err != nil {
				return pf.UnwrapGRPCError(r.err)
			}
			if r.resp.Stopped != nil {
				// TODO(johnny): The Rust capture FSM may hold after connector exit
				// until task.restart. We could clear this more promptly during that
				// cool-off interval if the protocol grows an explicit notification.
				c.container.Store(nil)
				return nil // Graceful drain complete.
			}
			logrus.WithFields(logrus.Fields{
				"shardId": shard.Spec().Id,
				"msg":     r.resp,
			}).Panic("unexpected Rust runtime message after Opened")
		}
	}
}

func (c *captureAppV2) Destroy() {
	if c.client != nil {
		_ = c.client.CloseSend()
		c.client = nil
	}
	c.taskBase.drop()
	c.taskBase.opsCancel()
}

func (c *captureAppV2) ReplayRange(_ consumer.Shard, _ pb.Journal, _, _ pb.Offset) message.Iterator {
	panic("runtime-v2: ReplayRange unreachable (capture has no Gazette message pipeline)")
}

func (c *captureAppV2) ReadThrough(_ pb.Offsets) (pb.Offsets, error) { return pb.Offsets{}, nil }
func (c *captureAppV2) BeginTxn(_ consumer.Shard) error              { panic("runtime-v2: BeginTxn unreachable") }
func (c *captureAppV2) ConsumeMessage(_ consumer.Shard, _ message.Envelope, _ *message.Publisher) error {
	panic("runtime-v2: ConsumeMessage unreachable")
}
func (c *captureAppV2) FinalizeTxn(_ consumer.Shard, _ *message.Publisher) error {
	panic("runtime-v2: FinalizeTxn unreachable")
}
func (c *captureAppV2) StartCommit(_ consumer.Shard, _ pf.Checkpoint, _ consumer.OpFutures) consumer.OpFuture {
	panic("runtime-v2: StartCommit unreachable")
}
func (c *captureAppV2) FinishedTxn(_ consumer.Shard, _ consumer.OpFuture) {}

func (c *captureAppV2) Coordinator() *shuffle.Coordinator {
	panic("runtime-v2: Coordinator unreachable")
}

func (c *captureAppV2) buildJoin() (*pr.Join, error) {
	// Use the term's already-captured ShardSpec and labeling rather than
	// shard.Spec(). Gazette's shard.Spec() re-acquires State.KS.Mu for read,
	// and since it aliases the lock we hold below, a recursive read-lock under
	// a queued Etcd writer would deadlock (Go blocks new readers once a writer
	// waits). c.term is a coherent topology snapshot, so this is also correct.
	var shardSpec = c.term.shardSpec

	var ks = c.host.service.State.KS
	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	var state = c.host.service.State
	var rev = ks.Header.Revision
	var asn, createRev, ok = primaryAssignment(state, shardSpec.Id)
	if !ok {
		return nil, fmt.Errorf("local shard %s does not have a PRIMARY assignment", shardSpec.Id)
	}

	return &pr.Join{
		EtcdModRevision: rev,
		Shards: []*pr.Join_Shard{{
			Id:                 shardSpec.Id.String(),
			Labeling:           &c.term.labels,
			Reactor:            &pb.ProcessSpec_ID{Zone: asn.MemberZone, Suffix: asn.MemberSuffix},
			EtcdCreateRevision: createRev,
		}},
		ShardIndex: 0,
	}, nil
}
