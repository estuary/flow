package runtime

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/message"
)

// materializeApp is a top-level Application which implements the materialization workflow.
type materializeApp struct {
	*taskReader[*pf.MaterializationSpec]
	acknowledged *pf.AsyncOperation
	client       pm.Connector_MaterializeClient
}

var _ application = (*materializeApp)(nil)

// newMaterializeApp returns a *Materialize Application.
func newMaterializeApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*materializeApp, error) {
	var base, err = newTaskBase[*pf.MaterializationSpec](host, shard, recorder, extractMaterializationSpec)
	if err != nil {
		return nil, err
	}
	go base.heartbeatLoop(shard)

	client, err := pm.NewConnectorClient(base.svc.Conn()).Materialize(shard.Context())
	if err != nil {
		base.drop()
		return nil, fmt.Errorf("starting materialize stream: %w", err)
	}

	return &materializeApp{
		taskReader:   newTaskReader[*pf.MaterializationSpec](base, shard),
		acknowledged: nil,
		client:       client,
	}, nil
}

func (m *materializeApp) RestoreCheckpoint(shard consumer.Shard) (_ pf.Checkpoint, _err error) {
	defer func() {
		if _err != nil {
			m.term.cancel()
		}
	}()

	if err := m.initTerm(shard); err != nil {
		return pf.Checkpoint{}, err
	}

	var requestExt = &pr.MaterializeRequestExt{
		LogLevel: m.term.labels.LogLevel,
	}
	if m.termCount == 1 {
		requestExt.RocksdbDescriptor = bindings.NewRocksDBDescriptor(m.recorder)
	}

	// Send Apply / receive Applied.
	_ = doSend[pm.Response](m.client, &pm.Request{
		Apply: &pm.Request_Apply{
			Materialization: m.term.taskSpec,
			Version:         m.term.labels.Build,
		},
		Internal: pr.ToInternal(requestExt),
	})
	if _, err := doRecv[pm.Response](m.client); err != nil {
		return pf.Checkpoint{}, err
	}

	// Send Open / receive Opened.
	_ = doSend[pm.Response](m.client, &pm.Request{
		Open: &pm.Request_Open{
			Materialization: m.term.taskSpec,
			Version:         m.term.labels.Build,
			Range:           &m.term.labels.Range,
			StateJson:       json.RawMessage("{}"),
		},
		Internal: pr.ToInternal(&pr.MaterializeRequestExt{LogLevel: m.term.labels.LogLevel}),
	})

	var opened, err = doRecv[pm.Response](m.client)
	if err != nil {
		return pf.Checkpoint{}, err
	}
	var openedExt = pr.FromInternal[pr.MaterializeResponseExt](opened.Internal)
	m.container.Store(openedExt.Container)

	// Send initial Acknowledge of the session.
	_ = doSend[pm.Response](m.client, &pm.Request{Acknowledge: &pm.Request_Acknowledge{}})

	m.acknowledged = pf.NewAsyncOperation()
	go readAcknowledged(m.client, m.acknowledged)

	// We must block until the very first Acknowledged is read (or errors).
	// If we didn't do this, then Request.Flush could potentially be sent before
	// the first Acknowledged is read, which is a protocol violation.
	return *opened.Opened.RuntimeCheckpoint, m.acknowledged.Err()
}

// ConsumeMessage implements Application.ConsumeMessage.
func (m *materializeApp) ConsumeMessage(shard consumer.Shard, envelope message.Envelope, pub *message.Publisher) error {
	var isr = envelope.Message.(pr.IndexedShuffleResponse)
	var keyPacked = isr.Arena.Bytes(isr.PackedKey[isr.Index])
	var docJson = isr.Arena.Bytes(isr.Docs[isr.Index])

	if message.GetFlags(isr.GetUUID()) == message.Flag_ACK_TXN {
		return nil // We just ignore the ACK documents.
	}

	var request = &pm.Request{
		Load: &pm.Request_Load{
			Binding:   uint32(isr.ShuffleIndex),
			KeyJson:   docJson[:len(docJson)-1], // Trim trailing newline.
			KeyPacked: keyPacked,
		},
	}
	if m.client.Send(request) == io.EOF {
		// We must await readAcknowledged() before attempting to read from `m.client`.
		if m.acknowledged.Err() != nil {
			return m.acknowledged.Err()
		}
		var _, err = doRecv[pm.Response](m.client)
		return err
	}

	return nil
}

func (m *materializeApp) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	// Precondition: m.acknowledged has resolved successfully and m.client is not being read.

	// Send Flush and await Flushed response.
	if err := doSend[pm.Response](m.client, &pm.Request{Flush: &pm.Request_Flush{}}); err != nil {
		return err
	}

	var resp, err = doRecv[pm.Response](m.client)
	if err != nil {
		return err
	} else if resp.Flushed == nil {
		return fmt.Errorf("expected Flushed (got %#v)", resp)
	}

	var flushedExt = pr.FromInternal[pr.MaterializeResponseExt](resp.Internal)
	if err := m.opsPublisher.PublishStats(*flushedExt.Flushed.Stats, pub.PublishUncommitted); err != nil {
		return fmt.Errorf("publishing stats: %w", err)
	}

	return nil
}

func (m *materializeApp) StartCommit(shard consumer.Shard, cp pf.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	ops.PublishLog(m.opsPublisher, ops.Log_debug,
		"StartCommit",
		"capture", m.term.labels.TaskName,
		"shard", m.term.shardSpec.Id,
		"build", m.term.labels.Build,
	)

	// Install a barrier such that we don't begin writing until `waitFor` has resolved.
	_ = m.recorder.Barrier(waitFor)

	// Tell materialize runtime we're starting to commit.
	if err := doSend[pm.Response](m.client, &pm.Request{
		StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &cp},
	}); err != nil {
		return client.FinishedOperation(err)
	}
	// Await it's StartedCommit, which tells us that all recovery log writes have been sequenced.
	if started, err := doRecv[pm.Response](m.client); err != nil {
		return client.FinishedOperation(err)
	} else if started.StartedCommit == nil {
		return client.FinishedOperation(fmt.Errorf("expected StartedCommit, but got %#v", started))
	}

	// Synchronously wait for another barrier which notifies when recovery log
	// writes have been durably recorded. This should be fast (milliseconds)
	// because we're only writing state & checkpoint updates.
	var barrier = m.recorder.Barrier(nil)
	if barrier.Err() != nil {
		return barrier
	}

	// Send Acknowledge.
	if err := doSend[pm.Response](m.client, &pm.Request{
		Acknowledge: &pm.Request_Acknowledge{},
	}); err != nil {
		return client.FinishedOperation(err)
	}

	// Start async read of Acknowledged.
	m.acknowledged = pf.NewAsyncOperation()
	go readAcknowledged(m.client, m.acknowledged)

	// Return `opAcknowledged` so that the next transaction will remain open
	// so long as the driver is still committing the current transaction.
	return m.acknowledged
}

// Destroy implements consumer.Store.Destroy
func (m *materializeApp) Destroy() {
	if m.client != nil {
		_ = m.client.CloseSend()
	}
	m.taskReader.drop()
	m.taskBase.opsCancel()
}

func (m *materializeApp) BeginTxn(shard consumer.Shard) error                    { return nil } // No-op.
func (m *materializeApp) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {}             // No-op.

func readAcknowledged(
	client pm.Connector_MaterializeClient,
	acknowledged *pf.AsyncOperation,
) (__err error) {
	defer func() {
		acknowledged.Resolve(__err)
	}()

	if resp, err := doRecv[pm.Response](client); err != nil {
		return err
	} else if resp.Acknowledged == nil {
		return fmt.Errorf("expected Acknowledged (got %#v)", resp)
	} else {
		return nil
	}
}

func extractMaterializationSpec(db *sql.DB, taskName string) (*pf.MaterializationSpec, error) {
	return catalog.LoadMaterialization(db, taskName)
}
