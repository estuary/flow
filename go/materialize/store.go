package materialize

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
)

type SqlTransaction struct {
	delegate *sql.Tx
	sql      *MaterializationSql
}

var _ TargetTransaction = (*SqlTransaction)(nil)

func (self *SqlTransaction) FetchExistingDocument(primaryKey []interface{}) (json.RawMessage, error) {
	var documentJson json.RawMessage
	stmt, err := self.delegate.Prepare(self.sql.FullDocumentQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	row := stmt.QueryRow(primaryKey...)
	err = row.Scan(&documentJson)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("Failed to query existing docuement: %w", err)
	} else if err == sql.ErrNoRows {
		return nil, nil
	} else {
		return documentJson, nil
	}
}

func (self *SqlTransaction) Store(extractedFields []interface{}, fullDocument json.RawMessage) error {
	stmt, err := self.delegate.Prepare(self.sql.InsertStatement)
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

type MaterializationStore struct {
	sqlConfig *MaterializationSql
	delegate  *consumer.SQLStore
}

var _ Target = (*MaterializationStore)(nil)

func (self *MaterializationStore) ProjectionPointers() []string {
	return self.sqlConfig.ProjectionPointers
}

func (self *MaterializationStore) PrimaryKeyFieldIndexes() []int {
	return self.sqlConfig.PrimaryKeyFieldIndexes
}

func (self *MaterializationStore) BeginTxn(ctx context.Context) (TargetTransaction, error) {
	txOpts := sql.TxOptions{
		// Ask the DB to surface serialization issues.
		// In PostgreSQL, this will fail the transaction if a serialization
		// order issue is encountered.
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	}
	tx, err := self.delegate.Transaction(ctx, &txOpts)
	if err != nil {
		return nil, err
	}

	sqlT := &SqlTransaction{
		delegate: tx,
		sql:      self.sqlConfig,
	}
	return sqlT, nil
}

func (self *MaterializationStore) StartCommit(shard consumer.Shard, checkpoint pc.Checkpoint, waitFor consumer.OpFutures) consumer.OpFuture {
	return self.delegate.StartCommit(shard, checkpoint, waitFor)
}

func (self *MaterializationStore) RestoreCheckpoint(shard consumer.Shard) (pc.Checkpoint, error) {
	return self.delegate.RestoreCheckpoint(shard)
}

func (self *MaterializationStore) Destroy() {
	if self != nil {
		self.delegate.Destroy()
	}
}
