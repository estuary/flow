package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocol"
	"github.com/estuary/flow/go/shuffle"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// Derive wires the high-level runtime of the derive consumer flow.
type Derive struct {
	delegate    *flow.WorkerHost
	readBuilder *shuffle.ReadBuilder
	mapper      flow.Mapper
	derivation  pf.CollectionSpec
	coordinator *shuffle.Coordinator

	*flow.Transaction
}

type recorderState struct {
	FSM            *recoverylog.FSM
	Author         recoverylog.Author
	CheckRegisters *pb.LabelSelector
}

var _ Application = (*Derive)(nil)

// NewDeriveApp builds and returns a *Derive Application.
func NewDeriveApp(
	service *consumer.Service,
	journals *keyspace.KeySpace,
	extractor *flow.WorkerHost,
	shard consumer.Shard,
	rec *recoverylog.Recorder,
) (*Derive, error) {
	var catalogURL, err = shardLabel(shard, labels.CatalogURL)
	if err != nil {
		return nil, err
	}
	catalog, err := flow.NewCatalog(catalogURL, rec.Dir)
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}
	derivation, err := shardLabel(shard, labels.Derivation)
	if err != nil {
		return nil, err
	}
	spec, err := catalog.LoadDerivedCollection(derivation)
	if err != nil {
		return nil, fmt.Errorf("loading collection spec: %w", err)
	}
	transforms, err := catalog.LoadTransforms(derivation)
	if err != nil {
		return nil, fmt.Errorf("loading transform specs: %w", err)
	}
	readBuilder, err := shuffle.NewReadBuilder(service, journals, shard, transforms)
	if err != nil {
		return nil, fmt.Errorf("NewReadBuilder: %w", err)
	}

	var mapper = flow.Mapper{
		Ctx:           shard.Context(),
		JournalClient: shard.JournalClient(),
		Journals:      journals,
	}

	// Write out recorder-state.json
	var (
		recorderStatePath = path.Join(rec.Dir, "recorder-state.json")
		recorderState     = recorderState{
			FSM:            rec.FSM,
			Author:         rec.Author,
			CheckRegisters: rec.CheckRegisters,
		}
	)
	if recorderStateFile, err := os.Create(recorderStatePath); err != nil {
		return nil, fmt.Errorf("creating recorder-state.json: %w", err)
	} else if err = json.NewEncoder(recorderStateFile).Encode(&recorderState); err != nil {
		return nil, fmt.Errorf("writing recorder-state.json: %w", err)
	} else if err = recorderStateFile.Close(); err != nil {
		return nil, fmt.Errorf("closing recorder-state.json: %w", err)
	}

	delegate, err := flow.NewWorkerHost(
		"derive",
		"--catalog", catalog.LocalPath(),
		"--derivation", derivation,
		"--dir", rec.Dir,
		"--recorder-state-path", recorderStatePath,
	)
	if err != nil {
		return nil, fmt.Errorf("starting derive flow-worker: %w", err)
	}

	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient(),
		pf.NewExtractClient(extractor.Conn))

	return &Derive{
		delegate:    delegate,
		readBuilder: readBuilder,
		mapper:      mapper,
		derivation:  spec,
		coordinator: coordinator,
		Transaction: nil,
	}, nil
}

// RestoreCheckpoint implements the Store interface, delegating to flow-worker.
func (a *Derive) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	if a.Transaction != nil {
		panic("unexpected !nil Transaction")
	}

	var cp, err = pf.NewDeriveClient(a.delegate.Conn).RestoreCheckpoint(shard.Context(), new(empty.Empty))
	if err != nil {
		return pc.Checkpoint{}, err
	}
	return *cp, nil
}

// BuildHints implements the Store interface, delegating to flow-worker.
func (a *Derive) BuildHints() (recoverylog.FSMHints, error) {
	if a.Transaction != nil {
		panic("unexpected !nil Transaction")
	}

	var hints, err = pf.NewDeriveClient(a.delegate.Conn).BuildHints(context.Background(), new(empty.Empty))
	if err != nil {
		return recoverylog.FSMHints{}, err
	}
	return *hints, nil
}

// Destroy implements the Store interface. It gracefully stops the flow-worker.
func (a *Derive) Destroy() {
	if a.Transaction != nil {
		panic("unexpected !nil Transaction")
	}
	if err := a.delegate.Stop(); err != nil {
		log.WithField("err", err).Error("failed to stop flow-worker")
	}
}

// BeginTxn begins a derive RPC transaction with the flow-worker.
func (a *Derive) BeginTxn(shard consumer.Shard) error {
	if a.Transaction != nil {
		panic("unexpected !nil Transaction")
	}

	var err error
	if a.Transaction, err = flow.NewTransaction(shard.Context(), a.delegate.Conn, &a.derivation, a.mapper.Map); err == nil {
		err = a.Transaction.Open()
	}
	if err != nil {
		return fmt.Errorf("BeginTxn: %w", err)
	}

	return nil
}

// FinishedTxn resets the current derive RPC.
func (a *Derive) FinishedTxn(_ consumer.Shard, _ consumer.OpFuture) {
	if a.Transaction == nil {
		panic("unexpected nil Transaction")
	}
	a.Transaction = nil
}

// StartReadingMessages delegates to shuffle.StartReadingMessages.
func (a *Derive) StartReadingMessages(shard consumer.Shard, cp pc.Checkpoint, ch chan<- consumer.EnvelopeOrError) {
	shuffle.StartReadingMessages(shard.Context(), a.readBuilder, cp, ch)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (a *Derive) ReplayRange(shard consumer.Shard, journal pb.Journal, begin pb.Offset, end pb.Offset) message.Iterator {
	return a.readBuilder.StartReplayRead(shard.Context(), journal, begin, end)
}

// ReadThrough delegates to shuffle.ReadThrough.
func (a *Derive) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	return a.readBuilder.ReadThrough(offsets)
}

// Coordinator returns the App's shared *shuffle.Coordinator.
func (a *Derive) Coordinator() *shuffle.Coordinator { return a.coordinator }

func shardLabel(shard consumer.Shard, label string) (string, error) {
	var values = shard.Spec().LabelSet.ValuesOf(label)
	if len(values) != 1 {
		return "", fmt.Errorf("expected single shard label %q (got %s)", label, values)
	}
	return values[0], nil
}
