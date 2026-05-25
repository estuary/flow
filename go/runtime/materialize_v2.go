package runtime

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/estuary/flow/go/shuffle"
	"github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// materializeAppV2 is the runtime-next backed materialization Application.
// It bypasses the Gazette transaction lifecycle entirely (drains
// StartReadingMessages, panics in BeginTxn / ConsumeMessage / FinalizeTxn /
// StartCommit) and instead drives the V2 protocol over the per-shard
// `Shard.Materialize` bidi stream. The Rust side handles all document I/O,
// shuffle, combining, publishing, and stat aggregation; the Go controller
// only manages session startup (Join → Joined → Task → Opened) and
// teardown (Stop → Stopped) per term.
type materializeAppV2 struct {
	*taskBase[*pf.MaterializationSpec]

	client pr.Shard_MaterializeClient
	// shuffleDir hosts per-shard shuffle files for this assignment.
	shuffleDir string

	// respCh is fed by a long-lived pump goroutine on `m.client.Recv()`.
	respCh <-chan recvResult
}

// recvResult is one outcome of m.client.Recv(): either a Materialize
// response or a terminal error (io.EOF on graceful close).
type recvResult struct {
	resp *pr.Materialize
	err  error
}

var _ application = (*materializeAppV2)(nil)

func newMaterializeAppV2(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*materializeAppV2, error) {
	if host.config.Flow.SidecarPort == 0 {
		return nil, fmt.Errorf("runtime-v2 requires --sidecar-port (or env SIDECAR_PORT)")
	}

	var shuffleDir, err = os.MkdirTemp("", "flow-runtime-v2-shuffle-")
	if err != nil {
		return nil, fmt.Errorf("creating runtime-v2 shuffle tempdir: %w", err)
	}

	var base *taskBase[*pf.MaterializationSpec]
	base, err = newTaskBaseV2[*pf.MaterializationSpec](host, shard, recorder, extractMaterializationSpec)
	if err != nil {
		_ = os.RemoveAll(shuffleDir)
		return nil, err
	}
	go base.heartbeatLoop(shard)

	var client pr.Shard_MaterializeClient
	client, err = pr.NewShardClient(base.svc.Conn()).Materialize(shard.Context())
	if err != nil {
		base.drop()
		_ = os.RemoveAll(shuffleDir)
		return nil, fmt.Errorf("opening V2 Shard.Materialize stream: %w", err)
	}

	// SessionLoop is the first message of the stream and lasts its lifetime.
	// It carries the RocksDB handle that runtime-next opens and reuses across
	// every leader session within this stream.
	var rocksDBDescriptor *pr.RocksDBDescriptor
	if recorder != nil {
		rocksDBDescriptor = bindings.NewRocksDBDescriptor(recorder)
	}
	_ = client.Send(&pr.Materialize{
		SessionLoop: &pr.SessionLoop{
			RocksdbDescriptor: rocksDBDescriptor,
		},
	})

	var respCh = make(chan recvResult, 4)
	go func() {
		defer close(respCh)
		for {
			var resp, err = client.Recv()
			// Bias towards send, but bail if full AND cancelled.
			select {
			case respCh <- recvResult{resp: resp, err: err}:
			default:
				select {
				case respCh <- recvResult{resp: resp, err: err}:
				case <-shard.Context().Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	return &materializeAppV2{
		taskBase:   base,
		client:     client,
		shuffleDir: shuffleDir,
		respCh:     respCh,
	}, nil
}

func (m *materializeAppV2) recv() (*pr.Materialize, error) {
	var r, ok = <-m.respCh
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return r.resp, r.err
}

func (m *materializeAppV2) RestoreCheckpoint(shard consumer.Shard) (pf.Checkpoint, error) {
	if err := shard.Context().Err(); err != nil {
		return pf.Checkpoint{}, err
	} else if err := m.initTerm(shard); err != nil {
		m.term.cancel()
		return pf.Checkpoint{}, err
	}
	// Note that RestoreCheckpoint is called at the transition from
	// STANDBY to PRIMARY: the shard is updated to PRIMARY only after its
	// return, and before StartReadingMessages.

	// Fail the shard if its runtime-v2 flag has been turned off. NewStore is
	// invoked only on the initial PRIMARY transition, so a publish that
	// flips the flag on a running shard cannot otherwise reroute it. We
	// surface a functional error so the controller restarts the shard,
	// at which point NewStore re-evaluates the flag and selects V1.
	if !useRuntimeV2(shard.Spec().LabelSet) {
		m.term.cancel()
		return pf.Checkpoint{}, fmt.Errorf(
			"runtime-v2 feature flag is unset but this shard is running the V2 materialize runtime; failing to force a restart")
	}

	// The Rust runtime owns checkpoint persistence, recovery, ACK intent
	// publishing, and the core transaction loop. Nothing to recover here.
	return pf.Checkpoint{}, nil
}

func (m *materializeAppV2) StartReadingMessages(shard consumer.Shard, _ pc.Checkpoint, _ *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {
	go m.runOneSession(shard, ch)
}

// runOneSession drives one session of the V2 protocol from end to end:
//
//  1. Load and marshal per-term task inputs.
//  2. Build & send Join once every task shard is PRIMARY-assigned, retrying
//     on disagreement until consensus.
//  3. Send Task and wait for Opened (which carries the connector Container).
//  4. Run the steady-state select loop until term cancellation, shard
//     cancellation, or a stream error.
//
// On term cancellation (spec update) we send Stop and wait for Stopped, then
// return nil so the framework loops back through RestoreCheckpoint. Any
// non-nil return is forwarded onto `ch` by the deferred sentinel before
// the channel is closed.
func (m *materializeAppV2) runOneSession(shard consumer.Shard, ch chan<- consumer.EnvelopeOrError) (err error) {
	defer func() {
		if err != nil {
			ch <- consumer.EnvelopeOrError{Error: err}
		}
		close(ch)
	}()

	var specBytes []byte
	if specBytes, err = m.term.taskSpec.Marshal(); err != nil {
		return fmt.Errorf("marshaling MaterializationSpec: %w", err)
	}

	// Run the Join/Joined protocol loop until consensus is met.
	var waitForRevision int64
	for {
		var ks = m.host.service.State.KS
		ks.Mu.RLock()
		err = ks.WaitForRevision(m.term.ctx, waitForRevision)
		ks.Mu.RUnlock()
		if err != nil {
			return fmt.Errorf("awaiting Etcd revision %d: %w", waitForRevision, err)
		}

		// Build Join from the current topology view, and send.
		var join, rev, err = buildLeaderfulJoin(m.host, m.term.shardSpec, m.shuffleDir)
		if err != nil {
			return fmt.Errorf("building Join: %w", err)
		} else if join == nil {
			waitForRevision = rev + 1
			continue
		}
		_ = m.client.Send(&pr.Materialize{Join: join})

		// Receive Joined and check consensus.
		var resp *pr.Materialize
		if resp, err = m.recv(); err != nil {
			return fmt.Errorf("receiving Joined: %w", pf.UnwrapGRPCError(err))
		} else if resp.Joined == nil {
			return fmt.Errorf("expected Joined, got %#v", resp)
		} else if resp.Joined.MaxEtcdRevision == 0 {
			break // Consensus.
		} else {
			// Disagreement: await the indicated revision, then re-poll topology.
			waitForRevision = resp.Joined.MaxEtcdRevision
		}
	}

	// Send Task.
	_ = m.client.Send(&pr.Materialize{
		Task: &pr.Task{
			Spec:            specBytes,
			Preview:         false,
			MaxTransactions: 0,
		},
	})

	// Receive Opened.
	var resp *pr.Materialize
	if resp, err = m.recv(); err != nil {
		return fmt.Errorf("receiving Opened: %w", pf.UnwrapGRPCError(err))
	} else if resp.Opened == nil {
		return fmt.Errorf("expected Opened, got %#v", resp)
	}
	m.container.Store(resp.Opened.Container)

	// Steady-state: drive teardown signals and surface stream errors.
	// termDone is nil-ed once we've sent Stop, so the case stops firing.
	// Future CloseNow plumbing slots in as another case alongside termDone.
	var termDone = m.term.ctx.Done()
	for {
		select {
		case <-termDone:
			// Spec update: initiate graceful drain. The leader replies with
			// Stopped, read from `respCh` below.
			_ = m.client.Send(&pr.Materialize{Stop: &pr.Stop{}})
			termDone = nil

		case <-shard.Context().Done():
			return shard.Context().Err() // Immediate, non-graceful shutdown.

		case r, ok := <-m.respCh:
			if !ok || r.err == io.EOF {
				return nil
			}
			if r.err != nil {
				return pf.UnwrapGRPCError(r.err)
			}
			if r.resp.Stopped != nil {
				return nil // Graceful drain complete.
			}
			logrus.WithFields(logrus.Fields{
				"shardId": shard.Spec().Id,
				"msg":     r.resp,
			}).Panic("unexpected Rust runtime message after Opened")
		}
	}
}

func (m *materializeAppV2) Destroy() {
	if m.client != nil {
		_ = m.client.CloseSend()
		m.client = nil
	}
	m.taskBase.drop()
	m.taskBase.opsCancel()
	_ = os.RemoveAll(m.shuffleDir)
}

func (m *materializeAppV2) ReplayRange(_ consumer.Shard, _ pb.Journal, _, _ pb.Offset) message.Iterator {
	panic("runtime-v2: ReplayRange unreachable (no Gazette message pipeline)")
}

func (m *materializeAppV2) ReadThrough(_ pb.Offsets) (pb.Offsets, error) {
	return pb.Offsets{}, nil
}

func (m *materializeAppV2) BeginTxn(_ consumer.Shard) error {
	panic("runtime-v2: BeginTxn unreachable (StartReadingMessages drains)")
}
func (m *materializeAppV2) ConsumeMessage(_ consumer.Shard, _ message.Envelope, _ *message.Publisher) error {
	panic("runtime-v2: ConsumeMessage unreachable (StartReadingMessages drains)")
}
func (m *materializeAppV2) FinalizeTxn(_ consumer.Shard, _ *message.Publisher) error {
	panic("runtime-v2: FinalizeTxn unreachable (StartReadingMessages drains)")
}
func (m *materializeAppV2) StartCommit(_ consumer.Shard, _ pf.Checkpoint, _ consumer.OpFutures) consumer.OpFuture {
	panic("runtime-v2: StartCommit unreachable (StartReadingMessages drains)")
}
func (m *materializeAppV2) FinishedTxn(_ consumer.Shard, _ consumer.OpFuture) {}

func (m *materializeAppV2) Coordinator() *shuffle.Coordinator {
	panic("runtime-v2: Coordinator unreachable (no Go shuffle)")
}

// buildLeaderfulJoin enumerates the task topology under read lock for a
// leader-ful task (materialization or derivation). If any shard does not yet
// have a PRIMARY assignment, it returns (nil, rev, nil) — a signal for the
// caller to await `rev+1` and retry; a hard error returns (nil, rev, err). A
// non-nil Join describes every shard, sorted by ShardID (which matches the
// proto's ordering contract on (key_begin, r_clock_begin) ascending). Both
// materializeAppV2 and deriveAppV2 share this exact consensus logic; the leader
// fail-stops a session on any disagreement, so the two callers must not drift.
func buildLeaderfulJoin(host *FlowConsumer, shardSpec *pf.ShardSpec, shuffleDir string) (*pr.Join, int64, error) {
	var ks = host.service.State.KS
	var itemPrefix = allocator.ItemKey(ks, taskShardPrefix(shardSpec.Id))

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()
	var state = host.service.State
	var rev = ks.Header.Revision

	if state.LocalMemberInd == -1 {
		return nil, rev, fmt.Errorf("local reactor member not present in Etcd state")
	}
	var selfEndpoint = state.Members[state.LocalMemberInd].
		Decoded.(allocator.Member).MemberValue.(*pc.ConsumerSpec).Endpoint
	var selfSidecar, err = host.config.SidecarEndpoint(selfEndpoint)
	if err != nil {
		return nil, rev, err
	}

	var (
		shards   []*pr.Join_Shard
		shardIdx = -1
	)
	for i, kv := range state.Items.Prefixed(itemPrefix) {
		var spec = kv.Decoded.(allocator.Item).ItemValue.(*pc.ShardSpec)
		var asn, createRev, ok = primaryAssignment(state, spec.Id)
		if !ok {
			return nil, rev, nil
		}
		var labeling, err = labels.ParseShardLabels(spec.LabelSet)
		if err != nil {
			return nil, rev, fmt.Errorf("parsing labels for %s: %w", spec.Id, err)
		}
		shards = append(shards, &pr.Join_Shard{
			Id:                 spec.Id.String(),
			Labeling:           &labeling,
			Reactor:            &pb.ProcessSpec_ID{Zone: asn.MemberZone, Suffix: asn.MemberSuffix},
			EtcdCreateRevision: createRev,
		})
		if spec.Id == shardSpec.Id {
			shardIdx = i
		}
	}
	if len(shards) == 0 {
		return nil, rev, nil
	}
	if shardIdx < 0 {
		return nil, rev, fmt.Errorf("local shard %s not in topology", shardSpec.Id)
	}

	var leaderEndpoint, ok = memberEndpoint(state, *shards[0].Reactor)
	if !ok {
		return nil, rev, fmt.Errorf("leader reactor %+v not present in Etcd state", shards[0].Reactor)
	}
	var leaderSidecar string
	if leaderSidecar, err = host.config.SidecarEndpoint(leaderEndpoint); err != nil {
		return nil, rev, err
	}

	return &pr.Join{
		EtcdModRevision:  rev,
		Shards:           shards,
		ShardIndex:       uint32(shardIdx),
		ShuffleDirectory: shuffleDir,
		ShuffleEndpoint:  selfSidecar,
		LeaderEndpoint:   leaderSidecar,
	}, rev, nil
}

// taskShardPrefix returns the keyspace directory under which all of the
// task's shard items live. ShardIDs are `<task-type>/<task-name>/<range>`;
// the prefix is everything up to and including the last `/`.
func taskShardPrefix(id pc.ShardID) string {
	var s = id.String()
	return s[:strings.LastIndexByte(s, '/')+1]
}

// memberEndpoint returns the ConsumerSpec endpoint for `id`. Caller must hold
// `state.KS.Mu` for read.
func memberEndpoint(state *allocator.State, id pb.ProcessSpec_ID) (pb.Endpoint, bool) {
	for _, kv := range state.Members {
		var member = kv.Decoded.(allocator.Member)
		if member.Zone != id.Zone || member.Suffix != id.Suffix {
			continue
		}
		var spec = member.MemberValue.(*pc.ConsumerSpec)
		return spec.Endpoint, true
	}
	return "", false
}

// primaryAssignment returns the slot-0 PRIMARY allocator.Assignment for `id`
// and its Etcd CreateRevision. Caller must hold `state.KS.Mu` for read.
func primaryAssignment(state *allocator.State, id pc.ShardID) (allocator.Assignment, int64, bool) {
	for _, akv := range state.KS.KeyValues.Prefixed(
		allocator.ItemAssignmentsPrefix(state.KS, id.String())) {
		var asn = akv.Decoded.(allocator.Assignment)
		var status = asn.AssignmentValue.(*pc.ReplicaStatus)
		if asn.Slot == 0 && status.Code == pc.ReplicaStatus_PRIMARY {
			return asn, akv.Raw.CreateRevision, true
		}
	}
	return allocator.Assignment{}, 0, false
}
