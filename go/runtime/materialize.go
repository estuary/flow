package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/connector"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	"github.com/estuary/flow/go/shuffle"
	"github.com/gogo/protobuf/types"
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
	// Embedded task reader scoped to current task version.
	// Initialized in RestoreCheckpoint.
	taskReader
	// Embedded processing state scoped to a current task version.
	// Initialized in RestoreCheckpoint.
	taskTerm
	// Accumulated stats of a current transaction.
	txnStats ops.Stats
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
			ops.PublishLog(m.opsPublisher, ops.Log_debug,
				"initialized processing term",
				"build", m.labels.Build,
				"checkpoint", cp,
				"checkpointSource", checkpointSource,
			)
		} else if !errors.Is(err, context.Canceled) {
			ops.PublishLog(m.opsPublisher, ops.Log_error,
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
	ops.PublishLog(m.opsPublisher, ops.Log_debug,
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
			binding.Collection.Name,
			binding.Collection.GetReadSchemaJson(),
			"", // Don't generate UUID placeholders.
			binding.Collection.Key,
			binding.FieldValuePtrs(),
		)
	}

	var configHandle = m.host.NetworkProxyServer.NetworkConfigHandle(m.shardSpec.Id, m.labels.Ports)
	// Start driver and Transactions RPC client.
	m.driver, err = connector.NewDriver(
		shard.Context(),
		m.spec.ConfigJson,
		m.spec.ConnectorType.String(),
		m.opsPublisher,
		m.host.Config.Flow.Network,
		configHandle,
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
	// the `checkpoint` recovered from the local recovery log store.
	if m.client.Opened().RuntimeCheckpoint != nil {
		cp = *m.client.Opened().RuntimeCheckpoint
		checkpointSource = "driver"
	} else {
		// Otherwise restore locally persisted checkpoint.
		if cp, err = m.store.RestoreCheckpoint(shard); err != nil {
			return pf.Checkpoint{}, fmt.Errorf("store.RestoreCheckpoint: %w", err)
		}
		checkpointSource = "recoveryLog"
	}

	removeOldOpsJournalAckIntents(cp.AckIntents)

	return cp, nil
}

// StartCommit implements consumer.Store.StartCommit
func (m *Materialize) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	driverCP, opAcknowledged, err := m.client.StartCommit(&cp)
	if err != nil {
		return client.FinishedOperation(err)
	} else if err = updateDriverCheckpoint(m.store, driverCP); err != nil {
		return client.FinishedOperation(err)
	}

	// Synchronously commit to the recovery log.
	// This should be fast (milliseconds) because we're not writing much data.
	// Then, write Acknowledge to the client.
	if opLog := m.store.StartCommit(shard, cp, waitFor); opLog.Err() != nil {
		return opLog
	} else if err = m.client.Acknowledge(); err != nil {
		return client.FinishedOperation(err)
	}

	ops.PublishLog(m.opsPublisher, ops.Log_debug, "started commit",
		"runtimeCheckpoint", cp,
		"driverCheckpoint", driverCP)

	// Return `opAcknowledged` so that the next transaction will remain open
	// so long as the driver is still committing the current transaction.
	return opAcknowledged
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

// BeginTxn implements Application.BeginTxn.
func (m *Materialize) BeginTxn(shard consumer.Shard) error {
	m.txnStats = ops.Stats{
		Shard:       ops.NewShardRef(m.labels),
		Timestamp:   types.TimestampNow(),
		TxnCount:    1,
		Materialize: make(map[string]*ops.Stats_Binding),
	}
	return nil
}

// ConsumeMessage implements Application.ConsumeMessage.
func (m *Materialize) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	var isr = envelope.Message.(pf.IndexedShuffleResponse)

	if message.GetFlags(isr.GetUUID()) == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	var keyPacked = isr.Arena.Bytes(isr.PackedKey[isr.Index])
	var keyJSON json.RawMessage // TODO(johnny).
	var doc = isr.Arena.Bytes(isr.Docs[isr.Index])

	return m.client.AddDocument(isr.ShuffleIndex, keyPacked, keyJSON, doc)
}

func (m *Materialize) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	if err := m.client.Flush(); err != nil {
		return err
	}
	ops.PublishLog(m.opsPublisher, ops.Log_debug, "flushed loads")

	var bindingStats, err = m.client.Store()
	if err != nil {
		return err
	}
	ops.PublishLog(m.opsPublisher, ops.Log_debug, "stored documents")

	for i, s := range bindingStats {
		mergeBinding(m.txnStats.Materialize, m.spec.Bindings[i].Collection.Name.String(), s)
	}
	m.txnStats.OpenSecondsTotal = time.Since(m.txnStats.GoTimestamp()).Seconds()

	if err := m.opsPublisher.PublishStats(m.txnStats, pub.PublishUncommitted); err != nil {
		return fmt.Errorf("publishing stats: %w", err)
	}
	return nil
}

// FinishedTxn implements Application.FinishedTxn.
func (m *Materialize) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {
	logTxnFinished(m.opsPublisher, op, shard)
}
