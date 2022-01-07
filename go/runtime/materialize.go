package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/shuffle"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// Materialize is a top-level Application which implements the materialization workflow.
type Materialize struct {
	// Client of the active driver transactions RPC.
	client *pm.TxnClient
	// Coordinator of shuffled reads for this materialization shard.
	coordinator *shuffle.Coordinator
	// FlowConsumer which owns this Materialize shard.
	host *FlowConsumer
	// Store delegate for persisting local checkpoints.
	store connectorStore
	// Specification under which the materialization is currently running.
	spec pf.MaterializationSpec
	// Stats are handled differently for materializations than they are for other task types,
	// because materializations don't have stats available until after the transaction starts to
	// commit. See comment below in StartCommit.
	statsPublisher *message.Publisher
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
	var coordinator = shuffle.NewCoordinator(shard.Context(), shard.JournalClient(), host.Builds)
	var store, err = newConnectorStore(recorder)
	if err != nil {
		return nil, fmt.Errorf("newConnectorStore: %w", err)
	}
	var clock = message.NewClock(time.Now().UTC())
	var statsPublisher = message.NewPublisher(shard.JournalClient(), &clock)

	var out = &Materialize{
		coordinator:    coordinator,
		host:           host,
		store:          store,
		statsPublisher: statsPublisher,
		client:         nil,                      // Initialized in RestoreCheckpoint.
		spec:           pf.MaterializationSpec{}, // Initialized in RestoreCheckpoint.
		taskReader:     taskReader{},             // Initialized in RestoreCheckpoint.
		taskTerm:       taskTerm{},               // Initialized in RestoreCheckpoint.
	}

	return out, nil
}

// RestoreCheckpoint establishes a driver connection and begins a Transactions RPC.
// It queries the driver to select from the latest local or driver-persisted checkpoint.
func (m *Materialize) RestoreCheckpoint(shard consumer.Shard) (cp pf.Checkpoint, err error) {
	if err = m.initTerm(shard, m.host); err != nil {
		return pf.Checkpoint{}, err
	}

	defer func() {
		if err == nil {
			m.Log(log.DebugLevel, log.Fields{
				"materialization": m.labels.TaskName,
				"shard":           m.shardSpec.Id,
				"build":           m.labels.Build,
				"checkpoint":      cp,
			}, "initialized processing term")
		} else {
			m.Log(log.ErrorLevel, log.Fields{
				"error": err.Error(),
			}, "failed to initialize processing term")
		}
	}()
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
	m.Log(log.DebugLevel, log.Fields{"spec": m.spec, "build": m.labels.Build},
		"loaded specification")

	if m.client != nil {
		err = m.client.Close()
		// Set the client to nil so that we don't try to close it again in Destroy should something
		// go wrong
		m.client = nil
		if err != nil && !errors.Is(err, context.Canceled) {
			return pf.Checkpoint{}, fmt.Errorf("stopping previous client: %w", err)
		}
		// The error could be a context cancelation, which we should ignore
		err = nil
	}

	if err = m.initReader(&m.taskTerm, shard, m.spec.TaskShuffles(), m.host); err != nil {
		return pf.Checkpoint{}, err
	}

	// Establish driver connection and start Transactions RPC.
	conn, err := materialize.NewDriver(
		shard.Context(),
		m.spec.EndpointType,
		m.spec.EndpointSpecJson,
		m.host.Config.Flow.Network,
		m.LogPublisher,
	)
	if err != nil {
		return pf.Checkpoint{}, fmt.Errorf("building endpoint driver: %w", err)
	}

	// Closure which builds a Combiner for a specified binding.
	var newCombinerFn = func(binding *pf.MaterializationSpec_Binding) (pf.Combiner, error) {
		var combiner, err = bindings.NewCombine(m.LogPublisher)
		if err != nil {
			return nil, err
		}
		return combiner, combiner.Configure(
			shard.FQN(),
			m.schemaIndex,
			binding.Collection.Collection,
			binding.Collection.SchemaUri,
			"", // Don't generate UUID placeholders.
			binding.Collection.KeyPtrs,
			binding.FieldValuePtrs(),
		)
	}

	m.client, err = pm.OpenTransactions(
		shard.Context(),
		conn,
		m.store.driverCheckpoint(),
		newCombinerFn,
		m.labels.Range,
		&m.spec,
		m.labels.Build,
	)
	if err != nil {
		return pf.Checkpoint{}, fmt.Errorf("opening transactions RPC: %w", err)
	}

	// If the store provided a Flow checkpoint, prefer that over
	// the |checkpoint| recovered from the local recovery log store.
	if b := m.client.Opened().FlowCheckpoint; len(b) != 0 {
		if err = cp.Unmarshal(b); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("unmarshal Opened.FlowCheckpoint: %w", err)
		}
	} else {
		// Otherwise restore locally persisted checkpoint.
		if cp, err = m.store.restoreCheckpoint(shard); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("store.RestoreCheckpoint: %w", err)
		}
	}

	return cp, nil
}

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	m.Log(log.DebugLevel, log.Fields{
		"materialization": m.labels.TaskName,
		"shard":           m.shardSpec.Id,
		"build":           m.labels.Build,
		"checkpoint":      cp,
	}, "StartCommit")

	var prepared, err = m.client.Prepare(cp)
	if err != nil {
		return client.FinishedOperation(err)
	}

	var commitOps = pm.CommitOps{
		DriverCommitted: client.NewAsyncOperation(),
		LogCommitted:    nil,
		Acknowledged:    client.NewAsyncOperation(),
	}

	// Arrange for our store to commit to its recovery log upon DriverCommitted.
	commitOps.LogCommitted = m.store.startCommit(shard, cp, prepared,
		consumer.OpFutures{commitOps.DriverCommitted: struct{}{}})

	stats, err := m.client.StartCommit(commitOps)
	if err != nil {
		return client.FinishedOperation(err)
	}
	var mapper = flow.NewMapper(shard.Context(), m.host.Service.Etcd, m.host.Journals, shard.FQN())

	// Stats are currently written using PublishCommitted for materializations, which means that
	// we cannot make any transactional guarantees about them. They might get published more than
	// once for the same transaction, or else not at all. The reason for this is that we don't
	// actually know the stats until we call client.StartCommit, which requires that we give it our
	// checkpoint. That checkpoint would need to include ack intents for the published stats
	// document if we were to make it transactional. So it's a chicken-and-egg problem.
	// A possible solution to this would be to add the ack intent to the checkpoint in FinalizeTxn,
	// but actually publish the stats message here using that sequencing info. This would
	// theoretically allow materialization stats to be published transactionally, but requires
	// exposing new functionality in the Publisher, so it is being left for a future improvement.
	var statsEvent = m.materializationStats(stats)
	m.statsPublisher.PublishCommitted(mapper.Map, m.StatsFormatter.FormatEvent(statsEvent))

	// Wait for any |waitFor| operations. In practice this is always empty.
	// It would contain pending journal writes, but materializations don't issue any.
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
	if m.client != nil {
		_ = m.client.Close()
	}
	m.taskTerm.destroy()
	m.store.destroy()
}

// Implementing shuffle.Store for Materialize
var _ shuffle.Store = (*Materialize)(nil)

// Coordinator implements shuffle.Store.Coordinator
func (m *Materialize) Coordinator() *shuffle.Coordinator {
	return m.coordinator
}

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

// FinalizeTxn implements Application.FinalizeTxn and is a no-op.
func (m *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	log.WithFields(log.Fields{"shard": shard.Spec().Id}).Trace("FinalizeTxn")
	return nil
}

// FinishedTxn implements Application.FinishedTxn.
func (m *Materialize) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {
	logTxnFinished(m.LogPublisher, op)
}
