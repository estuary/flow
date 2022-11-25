package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/shuffle"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Materialize is a top-level Application which implements the materialization workflow.
type Materialize struct {
	driver *connector.Driver
	// Client of the active driver transactions RPC.
	client *pm.TxnClient
	// FlowConsumer which owns this Materialize shard.
	host *FlowConsumer
	// Store delegate for persisting local checkpoints.
	store *consumer.JSONFileStore
	// Specification under which the materialization is currently running.
	spec pf.MaterializationSpec
	// Stats are handled differently for materializations than they are for other task types,
	// because materializations don't have stats available until after the transaction starts to
	// commit. pendingStats is populated during FinalizeTxn, and resolved in StartCommit.
	pendingStats message.PendingPublish
	// Embedded task reader scoped to current task version.
	// Initialized in RestoreCheckpoint.
	taskReader
	// Embedded processing state scoped to a current task version.
	// Initialized in RestoreCheckpoint.
	taskTerm
}

var _ Application = (*Materialize)(nil)

// NewMaterializeApp returns a new Materialize, which implements Application.
func NewMaterializeApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Materialize, error) {
	var store, err = newConnectorStore(recorder)
	if err != nil {
		return nil, fmt.Errorf("newConnectorStore: %w", err)
	}

	var out = &Materialize{
		host:       host,
		store:      store,
		driver:     nil,                      // Initialized in RestoreCheckpoint.
		client:     nil,                      // Initialized in RestoreCheckpoint.
		spec:       pf.MaterializationSpec{}, // Initialized in RestoreCheckpoint.
		taskReader: taskReader{},             // Initialized in RestoreCheckpoint.
		taskTerm:   taskTerm{},               // Initialized in RestoreCheckpoint.
	}

	return out, nil
}

// RestoreCheckpoint establishes a driver connection and begins a Transactions RPC.
// It queries the driver to select from the latest local or driver-persisted checkpoint.
func (m *Materialize) RestoreCheckpoint(shard consumer.Shard) (cp pf.Checkpoint, err error) {
	if err = m.initTerm(shard, m.host); err != nil {
		return pf.Checkpoint{}, err
	}

	var checkpointSource = "n/a"
	defer func() {
		if err == nil {
			ops.PublishLog(m.opsPublisher, pf.LogLevel_debug,
				"initialized processing term",
				"materialization", m.labels.TaskName,
				"shard", m.shardSpec.Id,
				"build", m.labels.Build,
				"checkpoint", cp,
				"checkpointSource", checkpointSource,
			)
		} else {
			ops.PublishLog(m.opsPublisher, pf.LogLevel_error,
				"failed to initialize processing term",
				"error", err,
			)
		}
	}()

	// Stop a previous Driver and Transactions client if it exists.
	if m.client != nil {
		if err = m.client.Close(); err != nil && !errors.Is(err, context.Canceled) {
			return pf.Checkpoint{}, fmt.Errorf("closing previous connector client: %w", err)
		}
		m.client = nil
	}
	if m.driver != nil {
		if err = m.driver.Close(); err != nil && !errors.Is(err, context.Canceled) {
			return pf.Checkpoint{}, fmt.Errorf("closing previous connector driver: %w", err)
		}
		m.driver = nil
	}

	// Load the current term's MaterializationSpec.
	err = m.build.Extract(func(db *sql.DB) error {
		materializationSpec, err := catalog.LoadMaterialization(db, m.labels.TaskName)
		if materializationSpec != nil {
			m.spec = *materializationSpec
		}
		return err
	})
	if err != nil {
		return pf.Checkpoint{}, err
	}
	ops.PublishLog(m.opsPublisher, pf.LogLevel_debug,
		"loaded specification",
		"spec", m.spec, "build", m.labels.Build)

	// Initialize for reading shuffled source collection journals.
	if err = m.initReader(&m.taskTerm, shard, m.spec.TaskShuffles(), m.host); err != nil {
		return pf.Checkpoint{}, err
	}

	// Closure which builds a Combiner for a specified binding.
	var newCombinerFn = func(binding *pf.MaterializationSpec_Binding) (pf.Combiner, error) {
		var combiner, err = bindings.NewCombine(m.opsPublisher)
		if err != nil {
			return nil, err
		}
		return combiner, combiner.Configure(
			shard.FQN(),
			binding.Collection.Collection,
			binding.Collection.SchemaJson,
			"", // Don't generate UUID placeholders.
			binding.Collection.KeyPtrs,
			binding.FieldValuePtrs(),
		)
	}

	// Start driver and Transactions RPC client.
	m.driver, err = connector.NewDriver(
		shard.Context(),
		m.spec.EndpointSpecJson,
		m.spec.EndpointType,
		m.opsPublisher,
		m.host.Config.Flow.Network,
	)
	if err != nil {
		return pf.Checkpoint{}, fmt.Errorf("building endpoint driver: %w", err)
	}

	// Open a Transactions RPC stream for the materialization.
	err = connector.WithUnsealed(m.driver, &m.spec, func(unsealed *pf.MaterializationSpec) error {
		var err error
		m.client, err = pm.OpenTransactions(
			shard.Context(),
			m.driver.MaterializeClient(),
			loadDriverCheckpoint(m.store),
			newCombinerFn,
			m.labels.Range,
			unsealed,
			m.labels.Build,
		)
		return err
	})
	if err != nil {
		return pf.Checkpoint{}, fmt.Errorf("opening transactions RPC: %w", err)
	}

	// If the store provided a Flow checkpoint, prefer that over
	// the |checkpoint| recovered from the local recovery log store.
	if b := m.client.Opened().FlowCheckpoint; len(b) != 0 {
		if err = cp.Unmarshal(b); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("unmarshal Opened.FlowCheckpoint: %w", err)
		}
		checkpointSource = "driver"
	} else {
		// Otherwise restore locally persisted checkpoint.
		if cp, err = m.store.RestoreCheckpoint(shard); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("store.RestoreCheckpoint: %w", err)
		}
		checkpointSource = "recoveryLog"
	}

	return cp, nil
}

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	ops.PublishLog(m.opsPublisher, pf.LogLevel_debug,
		"StartCommit",
		"materialization", m.labels.TaskName,
		"shard", m.shardSpec.Id,
		"build", m.labels.Build,
		"checkpoint", cp,
	)

	if driverCP, err := m.client.Prepare(cp); err != nil {
		return client.FinishedOperation(err)
	} else if err = updateDriverCheckpoint(m.store, driverCP); err != nil {
		return client.FinishedOperation(err)
	}

	var commitOps = pm.CommitOps{
		DriverCommitted: client.NewAsyncOperation(),
		LogCommitted:    nil,
		Acknowledged:    client.NewAsyncOperation(),
	}

	// Arrange for our store to commit to its recovery log upon DriverCommitted.
	commitOps.LogCommitted = m.store.StartCommit(shard, cp,
		consumer.OpFutures{commitOps.DriverCommitted: struct{}{}})

	stats, err := m.client.StartCommit(commitOps)
	if err != nil {
		return client.FinishedOperation(err)
	}

	// Now that we've drained the combiner, we're able to finish publishing the stats for this
	// transaction. This PendingPublish was initialized by the call to DeferPublishUncommitted
	// in FinalizeTxn.
	var statsEvent = m.materializationStats(stats)
	err = m.pendingStats.Resolve(m.StatsFormatter.FormatEvent(statsEvent))
	if err != nil {
		return client.FinishedOperation(fmt.Errorf("publishing stats: %w", err))
	}

	// Wait for any |waitFor| operations. This may include a stats publish of a prior transaction.
	for op := range waitFor {
		if op.Err() != nil {
			return client.FinishedOperation(fmt.Errorf("dependency failed: %w", op.Err()))
		}
	}

	// Return Acknowledged as the StartCommit future, which requires that it
	// resolve before the next transaction may begin to close.
	return commitOps.Acknowledged
}

func (m *Materialize) materializationStats(statsPerBinding []*pf.CombineAPI_Stats) StatsEvent {
	var stats = make(map[string]MaterializeBindingStats)
	for i, bindingStats := range statsPerBinding {
		// Skip bindings that didn't participate
		if bindingStats == nil {
			continue
		}
		var name = m.spec.Bindings[i].Collection.Collection.String()
		// It's possible for multiple bindings to use the same collection, in which case the
		// stats should be summed.
		var prevStats = stats[name]
		stats[name] = MaterializeBindingStats{
			Left:  prevStats.Left.with(bindingStats.Left),
			Right: prevStats.Right.with(bindingStats.Right),
			Out:   prevStats.Out.with(bindingStats.Out),
		}
	}
	var event = m.NewStatsEvent()
	event.Materialize = stats
	return event
}

// Destroy implements consumer.Store.Destroy
func (m *Materialize) Destroy() {
	if m.driver != nil {
		_ = m.driver.Close()
	}
	if m.client != nil {
		_ = m.client.Close()
	}
	m.taskTerm.destroy()
	m.store.Destroy()
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

// Implementing runtime.Application for Materialize
var _ Application = (*Materialize)(nil)

// BeginTxn implements Application.BeginTxn and is a no-op.
func (m *Materialize) BeginTxn(shard consumer.Shard) error {
	m.TxnOpened()
	return nil
}

// ConsumeMessage implements Application.ConsumeMessage.
func (m *Materialize) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	var isr = envelope.Message.(pf.IndexedShuffleResponse)

	if message.GetFlags(isr.GetUUID()) == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	// Find *Shuffle with equal pointer.
	var binding = -1 // Panic if no *Shuffle is matched.

	for i := range m.spec.Bindings {
		if &m.spec.Bindings[i].Shuffle == isr.Shuffle {
			binding = i
		}
	}

	var packedKey = isr.Arena.Bytes(isr.PackedKey[isr.Index])
	var doc = isr.Arena.Bytes(isr.DocsJson[isr.Index])
	return m.client.AddDocument(binding, packedKey, doc)
}

func (m *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	var mapper = flow.NewMapper(shard.Context(), m.host.Service.Etcd, m.host.Journals, shard.FQN())
	var journal, ct, ack, err = m.StatsFormatter.PrepareStatsJournal(mapper)
	if err != nil {
		return err
	}

	m.pendingStats, err = pub.DeferPublishUncommitted(journal, ct, ack)
	if err != nil {
		return fmt.Errorf("sequencing future stats message: %w", err)
	}
	log.WithFields(log.Fields{"shard": shard.Spec().Id}).Trace("FinalizeTxn")
	return nil
}

// FinishedTxn implements Application.FinishedTxn.
func (m *Materialize) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {
	logTxnFinished(m.opsPublisher, op)
}
