package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/fdb/tuple"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// SQLTransaction implements TargetTransaction for a generic SQL database
type SQLTransaction struct {
	tx     *sql.Tx
	fetch  *sql.Stmt
	upsert *sql.Stmt
}

func newSQLTransaction(tx *sql.Tx, sql *materializationSQL) (*SQLTransaction, error) {
	var fetch, err = tx.Prepare(sql.FullDocumentQuery)
	if err != nil {
		return nil, fmt.Errorf("preparing fetch statement: %w", err)
	}
	upsert, err := tx.Prepare(sql.InsertStatement)
	if err != nil {
		return nil, fmt.Errorf("preparing upsert statement: %w", err)
	}

	return &SQLTransaction{
		tx:     tx,
		fetch:  fetch,
		upsert: upsert,
	}, nil
}

var _ TargetTransaction = (*SQLTransaction)(nil)

// FetchExistingDocument implements TargetTransaction.FetchExistingDocument
func (txn *SQLTransaction) FetchExistingDocument(key tuple.Tuple) (json.RawMessage, error) {
	var row = txn.fetch.QueryRow(key.ToInterface()...)
	var doc json.RawMessage

	if err := row.Scan(&doc); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("querying existing document: %w", err)
	} else {
		return doc, nil
	}
}

// Store implements TargetTransaction.Store
func (txn *SQLTransaction) Store(doc json.RawMessage, packedKey []byte, fields tuple.Tuple) error {
	// We always put the full document as the last field.
	var all = make([]interface{}, len(fields)+1)
	for i, v := range fields {
		all[i] = v
	}
	all[len(fields)] = doc

	if _, err := txn.upsert.Exec(all...); err != nil {
		return fmt.Errorf("executing document upsert: %w", err)
	}
	return nil
}

// MaterializationStore implements the Target interface for a SQLStore
type MaterializationStore struct {
	sqlConfig *materializationSQL
	delegate  *consumer.SQLStore
}

var _ Target = (*MaterializationStore)(nil)

// ProjectionPointers implements Target.ProjectionPointers
func (store *MaterializationStore) ProjectionPointers() []string {
	return store.sqlConfig.ProjectionPointers
}

// PrimaryKeyFieldIndexes implements Target.PrimaryKeyFieldIndexes
func (store *MaterializationStore) PrimaryKeyFieldIndexes() []int {
	return store.sqlConfig.PrimaryKeyFieldIndexes
}

// BeginTxn implements Target.BeginTxn
func (store *MaterializationStore) BeginTxn(ctx context.Context) (TargetTransaction, error) {
	txOpts := sql.TxOptions{
		// Take the default Isolation level for the driver. For postgres, this will be
		// ReadUncommitted, and for sqlite it will be serializable. We can't support fully
		// serializable isolation with postgres at this time because of contention for the
		// gazette_checkpoints table. Sqlite's serializable isolation is simple enough that it
		// shouldn't present any issues.
		ReadOnly: false,
	}
	tx, err := store.delegate.Transaction(ctx, &txOpts)
	if err != nil {
		return nil, err
	}

	return newSQLTransaction(tx, store.sqlConfig)
}

// StartCommit implements consumer.Store.StartCommit
func (store *MaterializationStore) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	return store.delegate.StartCommit(shard, checkpoint, waitFor)
}

// RestoreCheckpoint implements consumer.Store.RestoreCheckpoint
func (store *MaterializationStore) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	return store.delegate.RestoreCheckpoint(shard)
}

// Destroy implements consumer.Store.Destroy
func (store *MaterializationStore) Destroy() {
	if store != nil {
		store.delegate.Destroy()
	}
}
