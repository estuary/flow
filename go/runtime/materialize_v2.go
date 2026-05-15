package runtime

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
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

	// 1. Load and marshal everything that doesn't depend on Etcd topology.
	var opsStatsJournal = m.term.labels.StatsJournal
	var opsStatsSpec *pf.CollectionSpec
	if opsStatsSpec, err = m.loadOpsCollectionSpec(opsStatsJournal); err != nil {
		return err
	}
	var specBytes []byte
	if specBytes, err = m.term.taskSpec.Marshal(); err != nil {
		return fmt.Errorf("marshaling MaterializationSpec: %w", err)
	}

	// 2. Build & send Join, repeating on disagreement until consensus.
	var waitForRevision int64
	for {
		var ks = m.host.service.State.KS
		ks.Mu.RLock()
		err = ks.WaitForRevision(m.term.ctx, waitForRevision)
		ks.Mu.RUnlock()
		if err != nil {
			return fmt.Errorf("awaiting Etcd revision %d: %w", waitForRevision, err)
		}
		var join, rev, err = m.buildJoin()
		if err != nil {
			return fmt.Errorf("building Join: %w", err)
		}
		if join == nil {
			waitForRevision = rev + 1
			continue
		}
		if err := m.client.Send(&pr.Materialize{Join: join}); err != nil {
			return fmt.Errorf("sending Join: %w", err)
		}
		var resp *pr.Materialize
		if resp, err = m.recv(); err != nil {
			return fmt.Errorf("receiving Joined: %w", pf.UnwrapGRPCError(err))
		}
		if resp.Joined == nil {
			return fmt.Errorf("expected Joined, got %#v", resp)
		}
		if resp.Joined.MaxEtcdRevision == 0 {
			break // Consensus.
		}
		// Disagreement: await the indicated revision, then re-poll topology.
		waitForRevision = resp.Joined.MaxEtcdRevision
	}

	// 3. Send Task and wait for Opened.
	if err := m.client.Send(&pr.Materialize{
		Task: &pr.Task{
			Spec:            specBytes,
			OpsStatsJournal: string(opsStatsJournal),
			OpsStatsSpec:    opsStatsSpec,
			Preview:         false,
			MaxTransactions: 0,
		},
	}); err != nil {
		return fmt.Errorf("sending Task: %w", err)
	}
	resp, err := m.recv()
	if err != nil {
		return fmt.Errorf("receiving Opened: %w", pf.UnwrapGRPCError(err))
	}
	if resp.Opened == nil {
		return fmt.Errorf("expected Opened, got %#v", resp)
	}
	m.container.Store(resp.Opened.Container)

	// 4. Steady-state: drive teardown signals and surface stream errors.
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

// loadOpsCollectionSpec resolves an ops CollectionSpec for `journal` from
// the current term's build DB. catalog.LoadCollectionForJournal inverts the
// `<collection>/<field>=<value>/...` shape produced by
// activate::ops_partition_spec via SQL prefix match against
// built_collections.
func (m *materializeAppV2) loadOpsCollectionSpec(journal pb.Journal) (*pf.CollectionSpec, error) {
	var spec *pf.CollectionSpec
	var build = m.host.builds.Open(m.term.labels.Build)
	defer build.Close()

	if err := build.Extract(func(db *sql.DB) error {
		var s, err = catalog.LoadCollectionForJournal(db, string(journal))
		if err != nil {
			return err
		}
		spec = s
		return nil
	}); err != nil {
		return nil, fmt.Errorf("loading ops CollectionSpec for %q: %w", journal, err)
	}
	return spec, nil
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

// buildJoin enumerates the task topology under read lock. If any shard does
// not yet have a PRIMARY assignment, it returns nil plus the observed Etcd
// revision so the caller can wait and try again. A non-nil Join describes every
// shard, sorted by ShardID (which matches the proto's ordering contract on
// (key_begin, r_clock_begin) ascending).
func (m *materializeAppV2) buildJoin() (*pr.Join, int64, error) {
	var ks = m.host.service.State.KS
	var itemPrefix = allocator.ItemKey(ks, taskShardPrefix(m.term.shardSpec.Id))

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()
	var state = m.host.service.State
	var rev = ks.Header.Revision

	if state.LocalMemberInd == -1 {
		return nil, rev, fmt.Errorf("local reactor member not present in Etcd state")
	}
	var selfEndpoint = state.Members[state.LocalMemberInd].
		Decoded.(allocator.Member).MemberValue.(*pc.ConsumerSpec).Endpoint
	var selfSidecar, err = m.host.config.SidecarEndpoint(selfEndpoint)
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
		if spec.Id == m.term.shardSpec.Id {
			shardIdx = i
		}
	}
	if len(shards) == 0 {
		return nil, rev, nil
	}
	if shardIdx < 0 {
		return nil, rev, fmt.Errorf("local shard %s not in topology", m.term.shardSpec.Id)
	}

	var leaderEndpoint, ok = memberEndpoint(state, *shards[0].Reactor)
	if !ok {
		return nil, rev, fmt.Errorf("leader reactor %+v not present in Etcd state", shards[0].Reactor)
	}
	var leaderSidecar string
	if leaderSidecar, err = m.host.config.SidecarEndpoint(leaderEndpoint); err != nil {
		return nil, rev, err
	}

	return &pr.Join{
		EtcdModRevision:  rev,
		Shards:           shards,
		ShardIndex:       uint32(shardIdx),
		ShuffleDirectory: m.shuffleDir,
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
