package consumer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"syscall"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
	"google.golang.org/grpc"
)

type deriveConfig struct {
	// Path to catalog.
	Catalog string
	// Name of collection which we're deriving.
	Derivation string
	// Unix domain socket to listen on.
	SocketPath string
	// Configuration for the worker's persistent state
	State deriveRecorderState
}

type deriveRecorderState struct {
	// FSM which details the persistent state manifest, including its recovery log.
	FSM *recoverylog.FSM
	// Author under which new operations should be fenced and recorded to the log.
	Author recoverylog.Author
	// Directory which roots the persistent state of this worker.
	Dir            string
	CheckRegisters *pb.LabelSelector
}

type worker struct {
	derivation string
	cmd        *exec.Cmd
	client     pf.DeriveClient
	conn       *grpc.ClientConn

	txn       pf.Derive_TransactionClient
	txnReadCh chan error
}

var _ consumer.Store = (*worker)(nil)

// RestoreCheckpoint implements the required Store interface.
func (s *worker) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	var cp, err = s.client.RestoreCheckpoint(shard.Context(), &empty.Empty{})
	if err != nil {
		return pc.Checkpoint{}, err
	}
	return *cp, nil
}

func (s *worker) readLoop(pub *message.Publisher) {
	var resp pf.TxnResponse
	for {
		if err := s.txn.RecvMsg(&resp); err != nil {
			s.txnReadCh <- fmt.Errorf("reading derive-worker txn response: %w", err)
			return
		}

		switch resp.State {
		case pf.DeriveTxnState_EXTEND:
			break
		case pf.DeriveTxnState_FLUSH:
			s.txnReadCh <- nil
			return
		default:
			s.txnReadCh <- fmt.Errorf("read unexpected DeriveTxnState %v", resp.State)
			return
		}
		// At this point, we know state is EXTEND.

		// TODO(johnny): Actually publish these, instead of just logging them.
		for doc := range resp.ExtendDocuments {
			log.WithField("derivation", s.derivation).
				WithField("parts", resp.ExtendLabels).
				Info("got document: ", doc)
		}
	}
}

func (s *worker) consumeMessage(shard consumer.Shard, env message.Envelope, pub *message.Publisher) error {
	if s.txn != nil {
		var txn, err = s.client.Transaction(shard.Context())
		if err != nil {
			return fmt.Errorf("failed to start a derive-worker transaction: %w", err)
		}

		s.txn, s.txnReadCh = txn, make(chan error, 1)
		go s.readLoop(pub)
	}

	var msg = env.Message.(RawJSONMessage)
	if uuid := msg.GetUUID(); message.GetFlags(uuid) == message.Flag_ACK_TXN {
		return nil // Ignore transaction acknowledgement messages.
	}

	// TODO(johnny): Batch up successive records of this same journal?

	return s.txn.Send(&pf.TxnRequest{
		State:           pf.DeriveTxnState_EXTEND,
		ExtendDocuments: [][]byte{msg.RawMessage},
		ExtendSource:    env.Journal.LabelSet.ValueOf(labels.Collection),
	})
}

func (s *worker) finalizeTxn() error {
	if err := s.txn.Send(&pf.TxnRequest{State: pf.DeriveTxnState_FLUSH}); err != nil {
		return fmt.Errorf("failed to flush derive-worker transaction: %w", err)
	}
	// Wait for readLoop() to finish and signal its exit.
	if err := <-s.txnReadCh; err != nil {
		return fmt.Errorf("derive-worker read loop failed: %w", err)
	}
	return nil
}

func (s *worker) StartCommit(_ consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	if err := s.txn.Send(&pf.TxnRequest{
		State:             pf.DeriveTxnState_PREPARE,
		PrepareCheckpoint: &checkpoint,
	}); err != nil {
		return client.FinishedOperation(
			fmt.Errorf("failed to send derive-worker transaction PREPARE: %w", err))
	} else if resp, err := s.txn.Recv(); err != nil {
		return client.FinishedOperation(
			fmt.Errorf("failed to read derive-worker transaction PREPARE: %w", err))
	} else if resp.State != pf.DeriveTxnState_PREPARE {
		return client.FinishedOperation(
			fmt.Errorf("unexpected derive-worker response (wanted PREPARE): %v", resp))
	}

	// Build a future to return now, that we'll resolve later
	// once the commit has finished (or failed).
	var future = client.NewAsyncOperation()

	// Take and nil current |txn|, allowing a new concurrent transaction to begin.
	var txn = s.txn
	s.txn, s.txnReadCh = nil, nil

	// Asynchronously:
	// - Wait for |waitFor| to resolve
	// - Signal derive-worker to un-gate it's prepared recovery-log write, committing the transaction
	// - Notify |future| on failure or success of the commit.
	go func() {
		defer txn.CloseSend()

		var err error
		for op := range waitFor {
			if err = op.Err(); err != nil {
				future.Resolve(err)
				return
			}
		}

		if err = txn.Send(&pf.TxnRequest{State: pf.DeriveTxnState_COMMIT}); err != nil {
			future.Resolve(fmt.Errorf("failed to send derive-worker COMMIT: %w", err))
		} else if resp, err := txn.Recv(); err != nil {
			future.Resolve(fmt.Errorf("failed to read derive-worker COMMIT: %w", err))
		} else if resp.State != pf.DeriveTxnState_COMMIT {
			future.Resolve(fmt.Errorf("unexpected derive-worker response (wanted COMMIT): %v", resp))
		} else if resp, err = txn.Recv(); err != io.EOF {
			future.Resolve(fmt.Errorf("expected derive-worker EOF, not: %v", resp))
		} else {
			future.Resolve(nil)
		}
	}()

	return future
}

// BuildHints returns FSMHints which may be played back to fully reconstruct the
// local filesystem state observed by this Recorder. It may block while pending
// operations sync to the log.
func (s *worker) BuildHints() (recoverylog.FSMHints, error) {
	var hints, err = s.client.BuildHints(context.Background(), &empty.Empty{})
	if err != nil {
		return recoverylog.FSMHints{}, err
	}
	return *hints, nil
}

func (s *worker) shutdown() error {
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("failed to close gRPC client: %w", err)
	} else if err = s.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to signal TERM to derive-worker: %w", err)
	} else if err = s.cmd.Wait(); err != nil {
		return fmt.Errorf("derive-worker exited with error: %w", err)
	} else if err = os.RemoveAll(s.cmd.Dir); err != nil {
		log.Fatalf("failed to remove derive-worker tmp directory %v: %v", s.cmd.Dir, err)
	}
	return nil
}

func (s *worker) Destroy() {
	if err := s.shutdown(); err != nil {
		log.WithField("err", err).Error("failed to shutdown derive-worker")
	}
}

// newWorker builds and returns a wrapped derive-worker process and gRPC connection.
func newWorker(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var catalogURL = shard.Spec().LabelSet.ValueOf(labels.CatalogURL)
	if catalogURL == "" {
		return nil, fmt.Errorf("expected label %q", labels.CatalogURL)
	}
	var derivation = shard.Spec().LabelSet.ValueOf(labels.Derivation)
	if derivation == "" {
		return nil, fmt.Errorf("expected label %q", labels.Derivation)
	}

	var (
		cfgPath    = path.Join(rec.Dir, "derive-config.json")
		socketPath = path.Join(rec.Dir, "unix-socket")
		cfg        = deriveConfig{
			Catalog:    catalogURL,
			Derivation: derivation,
			SocketPath: path.Join(rec.Dir, "unix-socket"),
			State: deriveRecorderState{
				FSM:            rec.FSM,
				Author:         rec.Author,
				Dir:            rec.Dir,
				CheckRegisters: rec.CheckRegisters,
			},
		}
	)
	// Write out derive-worker configuration.
	{
		var cfgFile, err = os.Create(cfgPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create derive-config.json: %w", err)
		} else if err = json.NewEncoder(cfgFile).Encode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to write config to derive-config.json: %w", err)
		} else if err = cfgFile.Close(); err != nil {
			return nil, err
		}
	}

	var cmd = exec.Command("derive-worker", "--config", cfgPath)
	cmd.Stderr = os.Stderr

	var stdout, err = cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe for derive-worker: %w", err)
	} else if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start derive-worker: %w", err)
	}
	log.Info("started derive-worker", "config", cfgPath)

	var br = bufio.NewReader(stdout)
	if ready, err := br.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("failed to read READY from derive-worker: %w", err)
	} else if ready != "READY\n" {
		return nil, fmt.Errorf("unexpected READY from derive-worker: %q", ready)
	}

	conn, err := grpc.DialContext(shard.Context(), "unix://"+socketPath, grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to dial derive-worker: %w", err)
	}

	return &worker{
		derivation: derivation,
		cmd:        cmd,
		conn:       conn,
		client:     pf.NewDeriveClient(conn),
	}, nil
}
