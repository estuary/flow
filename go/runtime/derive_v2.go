package runtime

import (
	"fmt"
	"io"
	"os"

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
	store_sqlite "go.gazette.dev/core/consumer/store-sqlite"
	"go.gazette.dev/core/message"
)

// deriveAppV2 is the runtime-next backed derivation Application. Like the
// materialization V2 controller it bypasses the Gazette transaction lifecycle
// entirely (drains StartReadingMessages, panics in BeginTxn / ConsumeMessage /
// FinalizeTxn / StartCommit) and instead drives the V2 protocol over the
// per-shard `Shard.Derive` bidi stream. The Rust side handles shuffle, the
// connector Read/Flush/Store cycle, output combining, journal publishing, and
// stat aggregation; the Go controller only manages session startup (Join →
// Joined → Task → Opened) and teardown (Stop → Stopped) per term.
//
// derive-sqlite is treated as a remote-authoritative connector: its state lives
// in a recorded SQLite VFS threaded to the connector, while shard zero's RocksDB
// is ephemeral. The leader recovers the authoritative checkpoint from the
// connector at Open, so the empty RocksDB scan is harmless.
type deriveAppV2 struct {
	*taskBase[*pf.CollectionSpec]

	client pr.Shard_DeriveClient
	// shuffleDir hosts per-shard shuffle files for this assignment.
	shuffleDir string
	// sqlite is non-nil only for a derive-sqlite task on shard zero; it owns
	// the recorded SQLite VFS that the connector opens.
	sqlite *store_sqlite.Store

	// respCh is fed by a long-lived pump goroutine on `m.client.Recv()`.
	respCh <-chan deriveRecvResult
}

// deriveRecvResult is one outcome of m.client.Recv(): either a Derive
// response or a terminal error (io.EOF on graceful close).
type deriveRecvResult struct {
	resp *pr.Derive
	err  error
}

var _ application = (*deriveAppV2)(nil)

func newDeriveAppV2(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*deriveAppV2, error) {
	if host.config.Flow.SidecarPort == 0 {
		return nil, fmt.Errorf("runtime-v2 requires --sidecar-port (or env SIDECAR_PORT)")
	}

	var shuffleDir, err = os.MkdirTemp("", "flow-runtime-v2-shuffle-")
	if err != nil {
		return nil, fmt.Errorf("creating runtime-v2 shuffle tempdir: %w", err)
	}

	var base *taskBase[*pf.CollectionSpec]
	base, err = newTaskBaseV2[*pf.CollectionSpec](host, shard, recorder, extractCollectionSpec)
	if err != nil {
		_ = os.RemoveAll(shuffleDir)
		return nil, err
	}
	go base.heartbeatLoop(shard)

	// derive-sqlite tasks thread a recorded SQLite VFS to the connector. Only
	// shard zero (which hosts the recovery log) builds it; non-zero shards have
	// a nil recorder. Register the VFS, create the `gazette_checkpoints` table,
	// then close the Go-side DB so Rust can reopen it through the registered VFS.
	var isSqlite = base.term.taskSpec.Derivation != nil &&
		base.term.taskSpec.Derivation.ConnectorType == pf.CollectionSpec_Derivation_SQLITE

	var sqlite *store_sqlite.Store
	if isSqlite && recorder != nil {
		if sqlite, err = store_sqlite.NewStore(recorder); err != nil {
			base.drop()
			_ = os.RemoveAll(shuffleDir)
			return nil, fmt.Errorf("building SQLite backing store: %w", err)
		} else if err = sqlite.Open(""); err != nil {
			sqlite.Destroy()
			base.drop()
			_ = os.RemoveAll(shuffleDir)
			return nil, fmt.Errorf("opening SQLite backing store: %w", err)
		} else if err = sqlite.SQLiteDB.Close(); err != nil {
			sqlite.Destroy()
			base.drop()
			_ = os.RemoveAll(shuffleDir)
			return nil, fmt.Errorf("closing SQLite DB in preparation for opening it again: %w", err)
		}
		sqlite.SQLiteDB = nil
	}

	var client pr.Shard_DeriveClient
	client, err = pr.NewShardClient(base.svc.Conn()).Derive(shard.Context())
	if err != nil {
		if sqlite != nil {
			sqlite.Destroy()
		}
		base.drop()
		_ = os.RemoveAll(shuffleDir)
		return nil, fmt.Errorf("opening V2 Shard.Derive stream: %w", err)
	}

	// SessionLoop is the first message of the stream and lasts its lifetime.
	// It carries the RocksDB handle that runtime-next opens and reuses across
	// every leader session within this stream. For derive-sqlite, shard zero's
	// RocksDB is intentionally ephemeral (a nil descriptor makes Rust open a
	// tempdir): the authoritative checkpoint is recovered from the connector at
	// Open. A non-SQLite shard zero records into the recovery log; non-zero
	// shards (nil recorder) always use an ephemeral tempdir.
	var rocksDBDescriptor *pr.RocksDBDescriptor
	if recorder != nil && !isSqlite {
		rocksDBDescriptor = bindings.NewRocksDBDescriptor(recorder)
	}
	_ = client.Send(&pr.Derive{
		SessionLoop: &pr.SessionLoop{
			RocksdbDescriptor: rocksDBDescriptor,
		},
	})

	var respCh = make(chan deriveRecvResult, 4)
	go func() {
		defer close(respCh)
		for {
			var resp, err = client.Recv()
			// Bias towards send, but bail if full AND cancelled.
			select {
			case respCh <- deriveRecvResult{resp: resp, err: err}:
			default:
				select {
				case respCh <- deriveRecvResult{resp: resp, err: err}:
				case <-shard.Context().Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	return &deriveAppV2{
		taskBase:   base,
		client:     client,
		shuffleDir: shuffleDir,
		sqlite:     sqlite,
		respCh:     respCh,
	}, nil
}

func (m *deriveAppV2) recv() (*pr.Derive, error) {
	var r, ok = <-m.respCh
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return r.resp, r.err
}

func (m *deriveAppV2) RestoreCheckpoint(shard consumer.Shard) (pf.Checkpoint, error) {
	if err := shard.Context().Err(); err != nil {
		return pf.Checkpoint{}, err
	} else if err := m.initTerm(shard); err != nil {
		m.term.cancel()
		return pf.Checkpoint{}, err
	}

	// Fail the shard if its runtime-v2 flag has been turned off. NewStore is
	// invoked only on the initial PRIMARY transition, so a publish that flips
	// the flag on a running shard cannot otherwise reroute it. We surface a
	// functional error so the controller restarts the shard, at which point
	// NewStore re-evaluates the flag and selects V1.
	if !useRuntimeV2(shard.Spec().LabelSet) {
		m.term.cancel()
		return pf.Checkpoint{}, fmt.Errorf(
			"runtime-v2 feature flag is unset but this shard is running the V2 derive runtime; failing to force a restart")
	}

	// The Rust runtime owns checkpoint persistence, recovery, ACK intent
	// publishing, and the core transaction loop. Nothing to recover here.
	return pf.Checkpoint{}, nil
}

func (m *deriveAppV2) StartReadingMessages(shard consumer.Shard, _ pc.Checkpoint, _ *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {
	go m.runOneSession(shard, ch)
}

// runOneSession drives one session of the V2 protocol from end to end, mirroring
// materializeAppV2.runOneSession: build & send Join until topology consensus,
// send Task and await Opened (which carries the connector Container), then run
// the steady-state select loop until term cancellation, shard cancellation, or
// a stream error. On term cancellation (spec update) we send Stop and await
// Stopped, then return nil so the framework loops back through RestoreCheckpoint.
func (m *deriveAppV2) runOneSession(shard consumer.Shard, ch chan<- consumer.EnvelopeOrError) (err error) {
	defer func() {
		if err != nil {
			ch <- consumer.EnvelopeOrError{Error: err}
		}
		close(ch)
	}()

	var specBytes []byte
	if specBytes, err = m.term.taskSpec.Marshal(); err != nil {
		return fmt.Errorf("marshaling CollectionSpec: %w", err)
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
		_ = m.client.Send(&pr.Derive{Join: join})

		// Receive Joined and check consensus.
		var resp *pr.Derive
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

	// Send Task. derive-sqlite shard zero threads its recorded SQLite VFS URI;
	// every other shard (and every non-SQLite task) sends an empty URI, which
	// the Rust shard ignores.
	var sqliteVfsUri string
	if m.sqlite != nil {
		sqliteVfsUri = m.sqlite.URIForDB("primary.db")
	}
	_ = m.client.Send(&pr.Derive{
		Task: &pr.Task{
			Spec:            specBytes,
			Preview:         false,
			MaxTransactions: 0,
			SqliteVfsUri:    sqliteVfsUri,
		},
	})

	// Receive Opened.
	var resp *pr.Derive
	if resp, err = m.recv(); err != nil {
		return fmt.Errorf("receiving Opened: %w", pf.UnwrapGRPCError(err))
	} else if resp.Opened == nil {
		return fmt.Errorf("expected Opened, got %#v", resp)
	}
	m.container.Store(resp.Opened.Container)

	// Steady-state: drive teardown signals and surface stream errors.
	// termDone is nil-ed once we've sent Stop, so the case stops firing.
	var termDone = m.term.ctx.Done()
	for {
		select {
		case <-termDone:
			// Spec update: initiate graceful drain. The leader replies with
			// Stopped, read from `respCh` below.
			_ = m.client.Send(&pr.Derive{Stop: &pr.Stop{}})
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

func (m *deriveAppV2) Destroy() {
	if m.client != nil {
		_ = m.client.CloseSend()
		m.client = nil
	}
	m.taskBase.drop()
	// Must destroy SQLite AFTER the task service: Rust holds the registered
	// VFS open until its service is dropped.
	if m.sqlite != nil {
		m.sqlite.Destroy()
	}
	m.taskBase.opsCancel()
	_ = os.RemoveAll(m.shuffleDir)
}

func (m *deriveAppV2) ReplayRange(_ consumer.Shard, _ pb.Journal, _, _ pb.Offset) message.Iterator {
	panic("runtime-v2: ReplayRange unreachable (no Gazette message pipeline)")
}

func (m *deriveAppV2) ReadThrough(_ pb.Offsets) (pb.Offsets, error) {
	return pb.Offsets{}, nil
}

func (m *deriveAppV2) BeginTxn(_ consumer.Shard) error {
	panic("runtime-v2: BeginTxn unreachable (StartReadingMessages drains)")
}
func (m *deriveAppV2) ConsumeMessage(_ consumer.Shard, _ message.Envelope, _ *message.Publisher) error {
	panic("runtime-v2: ConsumeMessage unreachable (StartReadingMessages drains)")
}
func (m *deriveAppV2) FinalizeTxn(_ consumer.Shard, _ *message.Publisher) error {
	panic("runtime-v2: FinalizeTxn unreachable (StartReadingMessages drains)")
}
func (m *deriveAppV2) StartCommit(_ consumer.Shard, _ pf.Checkpoint, _ consumer.OpFutures) consumer.OpFuture {
	panic("runtime-v2: StartCommit unreachable (StartReadingMessages drains)")
}
func (m *deriveAppV2) FinishedTxn(_ consumer.Shard, _ consumer.OpFuture) {}

func (m *deriveAppV2) Coordinator() *shuffle.Coordinator {
	panic("runtime-v2: Coordinator unreachable (no Go shuffle)")
}
