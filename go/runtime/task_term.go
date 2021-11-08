package runtime

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/shuffle"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/allocator"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/keyspace"
	"go.gazette.dev/core/message"
)

// taskTerm holds task state used by Capture, Derive and Materialize runtimes,
// which is re-initialized with each revision of the associated catalog task.
type taskTerm struct {
	shardSpec *pf.ShardSpec
	// Parsed and validated labels of the shard.
	labels labels.ShardLabeling
	// Resolved *Build of the task's build ID.
	build *flow.Build
	// Schema index of the task's build ID.
	schemaIndex *bindings.SchemaIndex
	// Logger used to publish logs that are scoped to this task.
	// It is embedded to allow directly calling .Log on a taskTerm.
	*LogPublisher
}

func (t *taskTerm) initTerm(shard consumer.Shard, host *FlowConsumer) error {
	var err error
	var lastLabels = t.labels

	t.shardSpec = shard.Spec()

	if t.labels, err = labels.ParseShardLabels(t.shardSpec.LabelSet); err != nil {
		return fmt.Errorf("parsing task shard: %w", err)
	}

	if t.build != nil && t.build.BuildID() == t.labels.Build {
		// Re-use this build.
	} else {
		if t.build != nil {
			// Cleanup a previous build.
			if err = t.build.Close(); err != nil {
				return err
			}
		}
		t.build = host.Builds.Open(t.labels.Build)
	}

	if t.schemaIndex, err = t.build.SchemaIndex(); err != nil {
		return err
	}

	var logsCollection *pf.CollectionSpec
	if err = t.build.Extract(func(db *sql.DB) error {
		logsCollection, err = catalog.LoadCollection(db, logCollection(t.labels.TaskName).String())
		return err
	}); err != nil {
		return err
	}

	// TODO(johnny): close old LogPublisher here, and in destroy() ?
	if t.LogPublisher, err = NewLogPublisher(
		t.labels,
		logsCollection,
		t.schemaIndex,
		shard.JournalClient(),
		flow.NewMapper(shard.Context(), host.Service.Etcd, host.Journals, shard.FQN()),
	); err != nil {
		return fmt.Errorf("creating log publisher: %w", err)
	}

	t.Log(log.InfoLevel, log.Fields{
		"labels":     t.labels,
		"lastLabels": lastLabels,
	}, "initialized catalog task term")

	return nil
}

func (t *taskTerm) destroy() {
	if err := t.build.Close(); err != nil {
		log.WithError(err).Error("failed to close build")
	}
}

type taskReader struct {
	// Builder of reads under the current task configuration.
	readBuilder *shuffle.ReadBuilder
	// readThroughMu guards an update of readBuilder from a
	// concurrent read it from ReadThrough().
	readThroughMu sync.Mutex
}

func (r *taskReader) initReader(
	term *taskTerm,
	shard consumer.Shard,
	shuffles []*pf.Shuffle,
	host *FlowConsumer,
) error {
	// Guard against a raced call to ReadThrough().
	r.readThroughMu.Lock()
	defer r.readThroughMu.Unlock()

	var err error
	r.readBuilder, err = shuffle.NewReadBuilder(
		host.Service,
		host.Journals,
		term.shardSpec.Id,
		shuffles,
		term.labels.Build,
	)
	if err != nil {
		return fmt.Errorf("NewReadBuilder: %w", err)
	}

	// Arrange for Drain to be called if the ShardSpec is updated.
	go signalOnSpecUpdate(host.Service.State.KS, shard, term.shardSpec, r.readBuilder.Drain)

	return nil
}

// StartReadingMessages delegates to shuffle.StartReadingMessages.
func (r *taskReader) StartReadingMessages(shard consumer.Shard, cp pc.Checkpoint,
	tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {

	log.WithFields(log.Fields{
		"shard": shard.Spec().Id,
	}).Debug("starting to read messages")

	shuffle.StartReadingMessages(shard.Context(), r.readBuilder, cp, tp, ch)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (r *taskReader) ReplayRange(shard consumer.Shard, journal pb.Journal,
	begin pb.Offset, end pb.Offset) message.Iterator {

	return shuffle.StartReplayRead(shard.Context(), r.readBuilder, journal, begin, end)
}

// ReadThrough maps |offsets| to the offsets read by this derivation.
// It delegates to readBuilder.ReadThrough. While other methods of this type are
// exclusively called from the shard's single processing loop, calls to
// ReadThrough come from the consumer's gRPC Stat API and may be raced.
// We must guard against a concurrent invocation.
func (r *taskReader) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	// Lock to guard against a raced call to initTerm().
	r.readThroughMu.Lock()
	var rb = r.readBuilder
	r.readThroughMu.Unlock()

	return rb.ReadThrough(offsets)
}

func signalOnSpecUpdate(ks *keyspace.KeySpace, shard consumer.Shard, spec *pf.ShardSpec, cb func()) {
	defer cb()
	var key = shard.FQN()

	ks.Mu.RLock()
	defer ks.Mu.RUnlock()

	for {
		// Pluck the ShardSpec out of the KeySpace, rather than using shard.Spec(),
		// to avoid a re-entrant read lock.
		var next *pf.ShardSpec
		if ind, ok := ks.Search(key); ok {
			next = ks.KeyValues[ind].Decoded.(allocator.Item).ItemValue.(*pf.ShardSpec)
		}

		if next != spec {
			return
		} else if err := ks.WaitForRevision(shard.Context(), ks.Header.Revision+1); err != nil {
			return
		}
	}
}
