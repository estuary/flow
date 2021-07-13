package runtime

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
)

// taskTerm holds task state used by Capture, Derive and Materialize runtimes,
// which is re-initialized with each revision of the associated catalog task.
type taskTerm struct {
	shardID pc.ShardID
	// Commons of the current task.
	commons *flow.Commons
	// Processing range owned by this shard.
	range_ pf.RangeSpec
	// Etcd revision of the current task.
	revision int64
	// Compiled index of the commons.
	schemaIndex *bindings.SchemaIndex
	// Current catalog task definition.
	task *pf.CatalogTask
}

func (t *taskTerm) initTerm(shard consumer.Shard, host *FlowConsumer) error {
	var err error
	var spec = shard.Spec()

	t.range_, err = labels.ParseRangeSpec(spec.LabelSet)
	if err != nil {
		return fmt.Errorf("parsing shard range: %w", err)
	}
	var taskName = spec.LabelSet.ValueOf(labels.TaskName)

	var revisionStr = spec.LabelSet.ValueOf(labels.TaskCreated)
	minTaskRevision, err := strconv.ParseInt(revisionStr, 10, 64)
	if err != nil {
		return fmt.Errorf("parsing %s: %w", labels.TaskCreated, err)
	}
	var lastRevision = t.revision
	t.task, t.commons, t.revision, err = host.Catalog.GetTask(shard.Context(), taskName, minTaskRevision)
	if err != nil {
		return err
	}

	t.schemaIndex, err = t.commons.SchemaIndex()
	if err != nil {
		return fmt.Errorf("building schema index: %w", err)
	}
	t.shardID = spec.Id

	log.WithFields(log.Fields{
		"task":         t.task.Name(),
		"shard":        t.shardID,
		"range":        t.range_.String(),
		"revision":     t.revision,
		"lastRevision": lastRevision,
	}).Info("initialized catalog task term")

	return nil
}

// shuffleTaskTerm holds task state used by the Derive and Materialize
// runtimes, which is re-initialized with each revision of the associated task.
// It extends taskTerm with initialization of shuffled reads.
type shuffleTaskTerm struct {
	taskTerm

	// Builder of reads under the current task configuration.
	readBuilder *shuffle.ReadBuilder
	// readThroughMu guards an update of readBuilder from a
	// concurrent read it from ReadThrough().
	readThroughMu sync.Mutex
	// Read shuffles extracted from the task definition.
	shuffles []*pf.Shuffle
}

func (t *shuffleTaskTerm) initShuffleTerm(shard consumer.Shard, host *FlowConsumer) error {
	var err = t.taskTerm.initTerm(shard, host)
	if err != nil {
		return err
	}

	t.shuffles = t.task.Shuffles()
	// Guard against a raced call to ReadThrough().
	t.readThroughMu.Lock()
	defer t.readThroughMu.Unlock()

	t.readBuilder, err = shuffle.NewReadBuilder(
		host.Service,
		host.Journals,
		t.shardID,
		t.shuffles,
		t.commons.CommonsId,
		t.revision,
	)
	if err != nil {
		return fmt.Errorf("NewReadBuilder: %w", err)
	}

	// Arrange for Drain to be called if the task definition is updated.
	host.Catalog.SignalOnTaskUpdate(shard.Context(),
		t.task.Name(), t.revision, t.readBuilder.Drain)

	return nil
}

// StartReadingMessages delegates to shuffle.StartReadingMessages.
func (t *shuffleTaskTerm) StartReadingMessages(shard consumer.Shard, cp pc.Checkpoint,
	tp *flow.Timepoint, ch chan<- consumer.EnvelopeOrError) {

	log.WithFields(log.Fields{
		"shard":    shard.Spec().Id,
		"revision": t.revision,
	}).Debug("starting to read messages")

	shuffle.StartReadingMessages(shard.Context(), t.readBuilder, cp, tp, ch)
}

// ReplayRange delegates to shuffle's StartReplayRead.
func (t *shuffleTaskTerm) ReplayRange(shard consumer.Shard, journal pb.Journal,
	begin pb.Offset, end pb.Offset) message.Iterator {

	return shuffle.StartReplayRead(shard.Context(), t.readBuilder, journal, begin, end)
}

// ReadThrough maps |offsets| to the offsets read by this derivation.
// It delegates to readBuilder.ReadThrough. While other methods of this type are
// exclusively called from the shard's single processing loop, calls to
// ReadThrough come from the consumer's gRPC Stat API and may be raced.
// We must guard against a concurrent invocation.
func (t *shuffleTaskTerm) ReadThrough(offsets pb.Offsets) (pb.Offsets, error) {
	// Lock to guard against a raced call to initTerm().
	t.readThroughMu.Lock()
	var rb = t.readBuilder
	t.readThroughMu.Unlock()

	return rb.ReadThrough(offsets)
}
