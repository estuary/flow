package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

// SQLTransaction implements TargetTransaction for a generic SQL database
type SQLTransaction struct {
	delegate *sql.Tx
	sql      *materializationSQL
}

var _ TargetTransaction = (*SQLTransaction)(nil)

// FetchExistingDocument implements TargetTransaction.FetchExistingDocument
func (transaction *SQLTransaction) FetchExistingDocument(primaryKey []interface{}) (json.RawMessage, error) {
	var documentJSON json.RawMessage
	stmt, err := transaction.delegate.Prepare(transaction.sql.FullDocumentQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	row := stmt.QueryRow(primaryKey...)
	err = row.Scan(&documentJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("Failed to query existing docuement: %w", err)
	} else if err == sql.ErrNoRows {
		return nil, nil
	} else {
		return documentJSON, nil
	}
}

// Store implements TargetTransaction.Store
func (transaction *SQLTransaction) Store(extractedFields []interface{}, fullDocument json.RawMessage) error {
	stmt, err := transaction.delegate.Prepare(transaction.sql.InsertStatement)
	if err != nil {
		return err
	}
	// We always put the full document as the last field
	allFields := append(extractedFields, fullDocument)
	_, err = stmt.Exec(allFields...)
	stmt.Close()
	if err != nil {
		return fmt.Errorf("Failed to execute insert statement: %w", err)
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

	sqlT := &SQLTransaction{
		delegate: tx,
		sql:      store.sqlConfig,
	}
	return sqlT, nil
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
