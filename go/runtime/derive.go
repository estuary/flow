package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path"

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

// Derive is a top-level Application which implements the derivation workflow.
type Derive struct {
	svc    *bindings.TaskService
	client pd.Connector_DeriveClient
	// FlowConsumer which owns this Derive shard.
	host *FlowConsumer
	// Instrumented RocksDB recorder.
	recorder *recoverylog.Recorder
	// Active derivation specification, updated in RestoreCheckpoint.
	collection *pf.CollectionSpec
	// Embedded processing state scoped to a current task version.
	// Updated in RestoreCheckpoint.
	taskTerm
	// Embedded task reader scoped to current task revision.
	// Also updated in RestoreCheckpoint.
	taskReader
	// Sqlite VFS backing store.
	sqlite *store_sqlite.Store
}

var _ Application = (*Derive)(nil)

// NewDeriveApp builds and returns a *Derive Application.
func NewDeriveApp(host *FlowConsumer, shard consumer.Shard, recorder *recoverylog.Recorder) (*Derive, error) {
	var derive = &Derive{
		svc:        nil, // Lazily initialized.
		client:     nil, // Lazily initialized.
		host:       host,
		recorder:   recorder,
		taskTerm:   taskTerm{},
		taskReader: taskReader{},
	}
	return derive, nil
}

// RestoreCheckpoint initializes a processing term for the derivation,
// configures the API binding delegate, and restores the last checkpoint.
// It implements the consumer.Store interface.
func (d *Derive) RestoreCheckpoint(shard consumer.Shard) (cp pf.Checkpoint, err error) {
	if err = d.initTerm(shard, d.host); err != nil {
		return pf.Checkpoint{}, err
	}

	defer func() {
		if err == nil {
			ops.PublishLog(d.opsPublisher, ops.Log_debug,
				"initialized processing term",
				"derivation", d.labels.TaskName,
				"shard", d.shardSpec.Id,
				"build", d.labels.Build,
				"checkpoint", cp,
			)
		} else if !errors.Is(err, context.Canceled) {
			ops.PublishLog(d.opsPublisher, ops.Log_error,
				"failed to initialize processing term",
				"error", err,
			)
		}
	}()

	err = d.build.Extract(func(db *sql.DB) error {
		d.collection, err = catalog.LoadCollection(db, d.labels.TaskName)
		return err
	})
	if err != nil {
		return pf.Checkpoint{}, err
	}
	if d.collection.Derivation == nil {
		return pf.Checkpoint{}, fmt.Errorf("this is an old task that needs to be updated")
	}
	ops.PublishLog(d.opsPublisher, ops.Log_debug,
		"loaded specification",
		"spec", d.collection, "build", d.labels.Build)

	if err = d.initReader(d.host, shard, d.collection, &d.taskTerm); err != nil {
		return pf.Checkpoint{}, err
	}

	if d.svc != nil {
		// No-op.
	} else if d.svc, err = bindings.NewTaskService(
		pr.TaskServiceConfig{
			AllowLocal:       d.host.Config.Flow.AllowLocal,
			ContainerNetwork: d.host.Config.Flow.Network,
			TaskName:         d.collection.Name.String(),
			UdsPath:          path.Join(d.recorder.Dir(), "socket"),
		},
		d.opsPublisher,
	); err != nil {
		return pf.Checkpoint{}, fmt.Errorf("creating task service: %w", err)
	}

	var requestExt = &pr.DeriveRequestExt{
		Labels: &d.labels,
		Open:   &pr.DeriveRequestExt_Open{},
	}

	if d.client != nil {
		// No-op
	} else if d.client, err = pd.NewConnectorClient(d.svc.Conn()).Derive(shard.Context()); err != nil {
		return pf.Checkpoint{}, fmt.Errorf("starting derivation stream: %w", err)
	} else {
		// On the very first open of the client, we thread through a SQLite or RocksDB
		// state backend which records into our recovery log.

		if d.collection.Derivation.ConnectorType == pf.CollectionSpec_Derivation_SQLITE {
			if d.sqlite, err = store_sqlite.NewStore(d.recorder); err != nil {
				return pf.Checkpoint{}, fmt.Errorf("building SQLite backing store: %w", err)
			} else if err = d.sqlite.Open(""); err != nil {
				return pf.Checkpoint{}, fmt.Errorf("opening SQLite backing store: %w", err)
			} else if err = d.sqlite.SQLiteDB.Close(); err != nil {
				return pf.Checkpoint{}, fmt.Errorf("closing SQLite DB in preparation for opening it again: %w", err)
			}
			d.sqlite.SQLiteDB = nil

			// Post-conditions:
			// * We have a registered SQLite VFS which can be addressed through d.sqlite.SQLiteURIValues,
			//   which is instrumented to record into our recovery log.
			// * The database was opened and a `gazette_checkpoints` table was created.
			// * We closed the actual database from the Go side and we won't use it again,
			//   but we WILL use the registered VFS from Rust.
		} else {
			requestExt.Open.RocksdbDescriptor = bindings.NewRocksDBDescriptor(d.recorder)
		}
	}

	if d.sqlite != nil {
		requestExt.Open.SqliteVfsUri = d.sqlite.URIForDB("primary.db")
	}
	_ = doSend(d.client, &pd.Request{
		Open: &pd.Request_Open{
			Collection: d.collection,
			Version:    d.labels.Build,
			Range:      &d.labels.Range,
		},
		Internal: pr.ToInternal(requestExt),
	})
	opened, err := doRecv(d.client)
	if err != nil {
		return pf.Checkpoint{}, err
	}
	var openedExt = pr.FromInternal[pr.DeriveResponseExt](opened.Internal)

	removeOldOpsJournalAckIntents(openedExt.Opened.RuntimeCheckpoint.AckIntents)

	return *openedExt.Opened.RuntimeCheckpoint, nil
}

// Destroy releases the API binding delegate, which also cleans up the associated
// Rust-held RocksDB and its files.
func (d *Derive) Destroy() {
	// `binding` could be nil if there was a failure during initialization.
	if d.client != nil {
		_ = d.client.CloseSend()
	}
	if d.svc != nil {
		d.svc.Drop()
	}
	if d.sqlite != nil {
		d.sqlite.Destroy()
	}
	d.taskTerm.destroy()
}

func (d *Derive) BeginTxn(shard consumer.Shard) error                    { return nil } // No-op.
func (d *Derive) FinishedTxn(shard consumer.Shard, op consumer.OpFuture) {}             // No-op.

// ConsumeMessage passes the message to the derive worker.
func (d *Derive) ConsumeMessage(_ consumer.Shard, env message.Envelope, _ *message.Publisher) error {
	var doc = env.Message.(pr.IndexedShuffleResponse)
	var uuid = doc.UuidParts[doc.Index]
	var keyPacked = doc.Arena.Bytes(doc.PackedKey[doc.Index])
	var docJson = doc.Arena.Bytes(doc.Docs[doc.Index])

	return doSend(d.client, &pd.Request{
		Read: &pd.Request_Read{
			Transform: uint32(doc.ShuffleIndex),
			Uuid:      &uuid,
			Shuffle: &pd.Request_Read_Shuffle{
				Packed: keyPacked,
			},
			DocJson: docJson,
		},
	})
}

// FinalizeTxn finishes and drains the derive worker transaction,
// and publishes each combined document to the derived collection.
func (d *Derive) FinalizeTxn(shard consumer.Shard, pub *message.Publisher) error {
	var mapper = flow.NewMapper(shard.Context(), d.host.Service.Etcd, d.host.Journals, shard.FQN())

	_ = d.client.Send(&pd.Request{Flush: &pd.Request_Flush{}})

	for {
		var response, err = doRecv(d.client)
		if err != nil {
			return err
		}
		var responseExt = pr.FromInternal[pr.DeriveResponseExt](response.Internal)

		if response.Published != nil {

			partitions, err := tuple.Unpack(responseExt.Published.PartitionsPacked)
			if err != nil {
				return fmt.Errorf("unpacking partitions: %w", err)
			}
			if _, err = pub.PublishUncommitted(mapper.Map, flow.Mappable{
				Spec:       d.collection,
				Doc:        response.Published.DocJson,
				PackedKey:  responseExt.Published.KeyPacked,
				Partitions: partitions,
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

// StartCommit implements the Store interface, and writes the current transaction
// as an atomic RocksDB WriteBatch, guarded by a write barrier.
func (d *Derive) StartCommit(_ consumer.Shard, cp pf.Checkpoint, waitFor client.OpFutures) client.OpFuture {
	ops.PublishLog(d.opsPublisher, ops.Log_debug,
		"StartCommit",
		"derivation", d.labels.TaskName,
		"shard", d.shardSpec.Id,
		"build", d.labels.Build,
		"checkpoint", cp,
	)

	// Install a barrier such that we don't begin writing until |waitFor| has resolved.
	_ = d.recorder.Barrier(waitFor)

	// Tell task service we're starting to commit.
	if err := doSend(d.client, &pd.Request{
		StartCommit: &pd.Request_StartCommit{
			RuntimeCheckpoint: &cp,
		},
	}); err != nil {
		return client.FinishedOperation(err)
	}
	// Await it's StartedCommit, which tells us that all recovery log writes have been sequenced.
	if started, err := doRecv(d.client); err != nil {
		return client.FinishedOperation(err)
	} else if started.StartedCommit == nil {
		return client.FinishedOperation(fmt.Errorf("expected StartedCommit, but got %#v", started))
	}
	// Another barrier which notifies when the WriteBatch
	// has been durably recorded to the recovery log.
	return d.recorder.Barrier(nil)
}

// ClearRegistersForTest delegates the request to its worker.
func (d *Derive) ClearRegistersForTest() error {
	_ = d.client.Send(&pd.Request{Reset_: &pd.Request_Reset{}})
	return nil
}

func doSend(client pd.Connector_DeriveClient, request *pd.Request) error {
	if err := client.Send(request); err == io.EOF {
		_, err = doRecv(client) // Read to obtain the *actual* error.
		return err
	} else if err != nil {
		panic(err) // gRPC client contract means this never happens
	}
	return nil
}

func doRecv(client pd.Connector_DeriveClient) (*pd.Response, error) {
	if r, err := client.Recv(); err != nil {
		return nil, pf.UnwrapGRPCError(err)
	} else {
		return r, nil
	}
}
