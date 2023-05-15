package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/estuary/flow/go/shuffle"
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
	// Current ShardSpec under which the task term is running.
	shardSpec *pf.ShardSpec
	// The taskTerm Context wraps the Shard Context, and is further cancelled
	// when the taskTerm's |shardSpec| has become out of date.
	ctx context.Context
	// Parsed and validated labels of the shard.
	labels ops.ShardLabeling
	// Resolved *Build of the task's build ID.
	build *flow.Build
	// ops.Publisher of ops.Logs and ops.Stats.
	opsPublisher *OpsPublisher
}

func (t *taskTerm) initTerm(shard consumer.Shard, host *FlowConsumer) error {
	var err error
	var lastLabels = t.labels

	t.shardSpec = shard.Spec()

	// Create a term Context which is cancelled if:
	// - The shard's Context is cancelled, or
	// - The ShardSpec is updated.
	// A cancellation of the term's Context doesn't invalidate the shard,
	// but does mean the current task term is done and a new one should be started.
	if t.ctx == nil || t.ctx.Err() != nil {
		var cancelFn context.CancelFunc
		t.ctx, cancelFn = context.WithCancel(shard.Context())
		go signalOnSpecUpdate(host.Service.State.KS, shard, t.shardSpec, cancelFn)
	}

	if t.labels, err = labels.ParseShardLabels(t.shardSpec.LabelSet); err != nil {
		return fmt.Errorf("parsing task shard labels: %w", err)
	}

	if t.build != nil && t.build.BuildID() == t.labels.Build {
		// Re-use this build.
	} else {
		if t.build != nil {
			if err = t.build.Close(); err != nil {
				return err
			}
		}
		t.build = host.Builds.Open(t.labels.Build)
	}

	logsCollectionSpec, statsCollectionSpec, err := resolveOpsCollections(t.build, t.labels.TaskName)
	if err != nil {
		return err
	}

	// OpsPublisher is instantiated once, but updates with labels of each term.
	if t.opsPublisher == nil {
		t.opsPublisher = NewOpsPublisher(
			host.LogPublisher,
			flow.NewMapper(shard.Context(), host.Service.Etcd, host.Journals, shard.FQN()),
		)
	}
	if err = t.opsPublisher.UpdateLabels(
		t.labels,
		logsCollectionSpec,
		statsCollectionSpec,
	); err != nil {
		return fmt.Errorf("creating ops publisher: %w", err)
	}

	ops.PublishLog(t.opsPublisher, ops.Log_info,
		"initialized catalog task term",
		"labels", t.labels,
		"lastLabels", lastLabels,
		"assignment", shard.Assignment().Decoded,
	)
	return nil
}

func (t *taskTerm) destroy() {
	if t.build != nil {
		if err := t.build.Close(); err != nil {
			log.WithError(err).Error("failed to close build")
		}
		t.build = nil
	}
}

type taskReader struct {
	// Coordinator of shuffled reads for this task.
	coordinator *shuffle.Coordinator
	// Builder of reads under the current task configuration.
	readBuilder *shuffle.ReadBuilder
	// mu guards an update of taskReader (within initReader),
	// from concurrent reads via ReadThrough() or Coordinator().
	mu sync.Mutex
}

func (r *taskReader) initReader(
	term *taskTerm,
	shard consumer.Shard,
	shuffles []*pf.Shuffle,
	host *FlowConsumer,
) error {
	// Guard against a raced call to ReadThrough() or Coordinator().
	r.mu.Lock()
	defer r.mu.Unlock()

	// Coordinator is instantiated once, and has the lifetime of the shard.
	if r.coordinator == nil {
		r.coordinator = shuffle.NewCoordinator(
			shard.Context(),
			term.opsPublisher,
			shard.JournalClient(),
		)
	}

	// Use the taskTerm's Context.Done as the |drainCh| monitored
	// by the ReadBuilder. When the term's context is cancelled,
	// reads of the ReadBuilder will gracefully drain themselves and
	// ultimately close the message channel of StartReadingMessages.
	var err error
	r.readBuilder, err = shuffle.NewReadBuilder(
		term.labels.Build,
		term.ctx.Done(), // Drain reads upon term cancellation.
		host.Journals,
		term.opsPublisher,
		host.Service,
		term.shardSpec.Id,
		shuffles,
	)
	if err != nil {
		return fmt.Errorf("NewReadBuilder: %w", err)
	}

	return nil
}

// StartReadingMessages delegates to shuffle.StartReadingMessages.
func (r *taskReader) StartReadingMessages(
	shard consumer.Shard,
	cp pc.Checkpoint,
	tp *flow.Timepoint,
	ch chan<- consumer.EnvelopeOrError,
) {
	shuffle.StartReadingMessages(shard.Context(), r.readBuilder, cp, tp, ch)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (r *taskReader) ReplayRange(
	shard consumer.Shard,
	journal pb.Journal,
	begin pb.Offset,
	end pb.Offset,
) message.Iterator {
	return shuffle.StartReplayRead(shard.Context(), r.readBuilder, journal, begin, end)
}

// ReadThrough maps |offsets| to the offsets read by this derivation.
// It delegates to readBuilder.ReadThrough. While other methods of this type are
// exclusively called from the shard's single processing loop, calls to
// ReadThrough come from the consumer's gRPC Stat API and may be raced.
// We must guard against a concurrent invocation.
func (r *taskReader) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	// Lock to guard against a raced call to initTerm().
	r.mu.Lock()
	var rb = r.readBuilder
	r.mu.Unlock()

	return rb.ReadThrough(offsets)
}

// Coordinator implements shuffle.Store.Coordinator
func (r *taskReader) Coordinator() *shuffle.Coordinator {
	// Lock to guard against a raced call to initTerm().
	r.mu.Lock()
	var c = r.coordinator
	r.mu.Unlock()

	return c
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

// We have transitioned away from putting logs & stats documents into individual tenant collections.
// We now put them in a single collection owned by the ops tenant, per dataplane. For historical
// reasons the ops collections for logs & stats are added in during builds, currently via the
// assemble crate. Builds that were done prior to switching away from per-tenant collections will
// have their ops collections in the form of "ops/tenant/{logs,stats}", and builds after will have
// their ops collections like "ops.dataplane/{logs,stats}"
//
// We resolve that difference here. Collections from the build are extracted perferentially by the
// "new" form of the collection name and returned as-is if found. If the new ops collections aren't
// in the build, we fall back to looking for the "old" form of the collections.
//
// New journals will be created automatically to partition the single logs/stats collections for
// each task. We need to make sure that old builds with per-tenant ops collections have their
// partitions created correctly for the new singular collections, so the relevant parts of the
// retrieved ops specs for old builds are also updated here.
func resolveOpsCollections(build *flow.Build, taskName string) (logs *pf.CollectionSpec, stats *pf.CollectionSpec, err error) {
	logsCollectionName := "ops.us-central1.v1/logs"
	statsCollectionsName := "ops.us-central1.v1/stats"

	var mustUpdate bool

	if err = build.Extract(func(db *sql.DB) error {
		if logs, err = catalog.LoadCollection(db, logsCollectionName); err != nil {
			// Fall back to looking for the old form of the collection if not found by the new one,
			// and make sure the relevant parts of the spec are updated for the journal that may be
			// created for it.
			mustUpdate = true
			if logs, err = catalog.LoadCollection(db, ops.LogCollection(taskName).String()); err != nil {
				return fmt.Errorf("loading logs collection: %w", err)
			}
		}

		if stats, err = catalog.LoadCollection(db, statsCollectionsName); err != nil {
			mustUpdate = true
			if stats, err = catalog.LoadCollection(db, ops.StatsCollection(taskName).String()); err != nil {
				return fmt.Errorf("loading stats collection: %w", err)
			}
		}

		return nil
	}); err != nil {
		return nil, nil, err
	}

	update := func(spec *pf.CollectionSpec, collectionName string) {
		spec.Name = pf.Collection(collectionName)
		spec.PartitionTemplate.Name = pb.Journal(collectionName)
		spec.PartitionTemplate.LabelSet.SetValue(labels.Collection, collectionName)
	}

	// If the ops collections were already the new form we won't force updates on them since they
	// should already be correct, although it probably wouldn't hurt anything if we did.
	if mustUpdate {
		update(logs, logsCollectionName)
		update(stats, statsCollectionsName)
	}

	return
}
