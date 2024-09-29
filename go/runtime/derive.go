package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/bindings"
	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/protocols/catalog"
	pd "github.com/estuary/flow/go/protocols/derive"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/estuary/flow/go/protocols/ops"
	pr "github.com/estuary/flow/go/protocols/runtime"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	store_sqlite "go.gazette.dev/core/consumer/store-sqlite"
	"go.gazette.dev/core/message"
)

// deriveApp is a top-level Application which implements the derivation workflow.
type deriveApp struct {
	*taskReader[*pf.CollectionSpec]
	client      pd.Connector_DeriveClient
	sqlite      *store_sqlite.Store
	watch       *client.WatchedList
	watchCancel context.CancelFunc
}

var _ application = (*deriveApp)(nil)

// newDeriveApp builds and returns a *Derive Application.
func newDeriveApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*deriveApp, error) {
	var base, err = newTaskBase[*pf.CollectionSpec](host, shard, recorder, extractCollectionSpec)
	if err != nil {
		return nil, err
	}

	var sqlite *store_sqlite.Store

	if base.term.taskSpec.Derivation.ConnectorType == pf.CollectionSpec_Derivation_SQLITE {
		// Post-conditions of this block:
		// * We have a registered SQLite VFS which can be addressed through sqlite.SQLiteURIValues,
		//   which is instrumented to record into our recovery log.
		// * The database was opened and a `gazette_checkpoints` table was created.
		// * We closed the actual database from the Go side and we won't use it again,
		//   but we WILL use the registered VFS from Rust.
		if sqlite, err = store_sqlite.NewStore(base.recorder); err != nil {
			base.drop()
			return nil, fmt.Errorf("building SQLite backing store: %w", err)
		} else if err = sqlite.Open(""); err != nil {
			sqlite.Destroy()
			base.drop()
			return nil, fmt.Errorf("opening SQLite backing store: %w", err)
		} else if err = sqlite.SQLiteDB.Close(); err != nil {
			sqlite.Destroy()
			base.drop()
			return nil, fmt.Errorf("closing SQLite DB in preparation for opening it again: %w", err)
		}
		sqlite.SQLiteDB = nil
	}

	client, err := pd.NewConnectorClient(base.svc.Conn()).Derive(shard.Context())
	if err != nil {
		base.drop()
		return nil, fmt.Errorf("starting derivation stream: %w", err)
	}

	return &deriveApp{
		taskReader: newTaskReader[*pf.CollectionSpec](base, shard),
		client:     client,
		sqlite:     sqlite,
	}, nil
}

func (d *deriveApp) RestoreCheckpoint(shard consumer.Shard) (_ pf.Checkpoint, _err error) {
	defer func() {
		if _err != nil {
			d.term.cancel()
		}
	}()

	if err := d.initTerm(shard); err != nil {
		return pf.Checkpoint{}, err
	}

	var watchCtx context.Context
	if d.watchCancel != nil {
		d.watchCancel() // Cancel watch of previous term.
	}
	watchCtx, d.watchCancel = context.WithCancel(shard.Context())

	d.watch = client.NewWatchedList(
		watchCtx,
		shard.JournalClient(),
		flow.CollectionWatchRequest(d.term.taskSpec),
		nil,
	)
	select {
	case err := <-d.watch.UpdateCh():
		if err != nil {
			return pf.Checkpoint{}, fmt.Errorf("initializing journal watch: %w", err)
		}
	case <-d.term.ctx.Done():
		return pf.Checkpoint{}, d.term.ctx.Err()
	}

	var requestExt = &pr.DeriveRequestExt{
		LogLevel: d.term.labels.LogLevel,
		Open:     &pr.DeriveRequestExt_Open{},
	}
	if d.sqlite != nil {
		requestExt.Open.SqliteVfsUri = d.sqlite.URIForDB("primary.db")
	} else if d.termCount == 1 {
		requestExt.RocksdbDescriptor = bindings.NewRocksDBDescriptor(d.recorder)
	}

	_ = doSend[pd.Response](d.client, &pd.Request{
		Open: &pd.Request_Open{
			Collection: d.term.taskSpec,
			Version:    d.term.labels.Build,
			Range:      &d.term.labels.Range,
			StateJson:  json.RawMessage("{}"),
		},
		Internal: pr.ToInternal(requestExt),
	})

	var opened, err = doRecv[pd.Response](d.client)
	if err != nil {
		return pf.Checkpoint{}, err
	}
	var openedExt = pr.FromInternal[pr.DeriveResponseExt](opened.Internal)
	d.container.Store(openedExt.Container)
	if d.termCount == 1 {
		// See comment in capture.go
		d.taskBase.StartTaskHeartbeatLoop(shard, openedExt.Container)
	}

	return *openedExt.Opened.RuntimeCheckpoint, nil
}

// ConsumeMessage forwards a Read to the derive runtime.
func (d *deriveApp) ConsumeMessage(_ consumer.Shard, env message.Envelope, _ *message.Publisher) error {
	var isr = env.Message.(pr.IndexedShuffleResponse)
	var uuid = isr.UuidParts[isr.Index]
	var keyPacked = isr.Arena.Bytes(isr.PackedKey[isr.Index])
	var docJson = isr.Arena.Bytes(isr.Docs[isr.Index])

	return doSend[pd.Response](d.client, &pd.Request{
		Read: &pd.Request_Read{
			Transform: uint32(isr.ShuffleIndex),
			Uuid:      &uuid,
			Shuffle:   &pd.Request_Read_Shuffle{Packed: keyPacked},
			DocJson:   docJson,
		},
	})
}

// FinalizeTxn finishes and drains the derive runtime transaction,
// and publishes each document to the derived collection.
func (d *deriveApp) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	var mapper = flow.NewMapper(shard.Context(), shard.JournalClient())

	_ = d.client.Send(&pd.Request{Flush: &pd.Request_Flush{}})

	for {
		var response, err = doRecv[pd.Response](d.client)
		if err != nil {
			return err
		}
		var responseExt = pr.FromInternal[pr.DeriveResponseExt](response.Internal)

		if response.Published != nil {
			var partitions, err = tuple.Unpack(responseExt.Published.PartitionsPacked)
			if err != nil {
				return fmt.Errorf("unpacking partitions: %w", err)
			}
			if _, err = pub.PublishUncommitted(mapper.Map, flow.Mappable{
				Spec:       d.term.taskSpec,
				Doc:        response.Published.DocJson,
				PackedKey:  responseExt.Published.KeyPacked,
				Partitions: partitions,
				List:       d.watch,
			}); err != nil {
				return fmt.Errorf("publishing document: %w", err)
			}

		} else if response.Flushed != nil {
			if err := d.opsPublisher.PublishStats(*responseExt.Flushed.Stats, pub.PublishUncommitted); err != nil {
				return fmt.Errorf("publishing stats: %w", err)
			}
			return nil
		}
	}
}

func (d *deriveApp) StartCommit(_ consumer.Shard, cp pf.Checkpoint, waitFor client.OpFutures) client.OpFuture {
	ops.PublishLog(d.opsPublisher, ops.Log_debug,
		"StartCommit",
		"derivation", d.term.labels.TaskName,
		"shard", d.term.shardSpec.Id,
		"build", d.term.labels.Build,
	)

	// Install a barrier such that we don't begin writing until `waitFor` has resolved.
	_ = d.recorder.Barrier(waitFor)

	// Tell derive runtime we're starting to commit.
	if err := doSend[pd.Response](d.client, &pd.Request{
		StartCommit: &pd.Request_StartCommit{RuntimeCheckpoint: &cp},
	}); err != nil {
		return client.FinishedOperation(err)
	}
	// Await it's StartedCommit, which tells us that all recovery log writes have been sequenced.
	if started, err := doRecv[pd.Response](d.client); err != nil {
		return client.FinishedOperation(err)
	} else if started.StartedCommit == nil {
		return client.FinishedOperation(fmt.Errorf("expected StartedCommit, but got %#v", started))
	}
	// Another barrier which notifies when the WriteBatch
	// has been durably recorded to the recovery log.
	return d.recorder.Barrier(nil)
}

// Destroy releases the API binding delegate, which also cleans up the associated
// Rust-held RocksDB and its files.
func (d *deriveApp) Destroy() {
	if d.client != nil {
		_ = d.client.CloseSend()
	}
	d.taskReader.drop()

	// Must drop after task service.
	if d.sqlite != nil {
		d.sqlite.Destroy()
	}
	d.taskBase.opsCancel()
}

func (d *deriveApp) ClearRegistersForTest() error {
	_ = d.client.Send(&pd.Request{Reset_: &pd.Request_Reset{}})
	return nil
}

func (d *deriveApp) BeginTxn(shard consumer.Shard) error                    { return nil } // No-op.
func (d *deriveApp) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {}             // No-op.

func extractCollectionSpec(db *sql.DB, taskName string) (*pf.CollectionSpec, error) {
	return catalog.LoadCollection(db, taskName)
}
