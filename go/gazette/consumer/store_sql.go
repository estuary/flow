package consumer

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"go.gazette.dev/core/broker/client"
	pc "go.gazette.dev/core/consumer/protocol"
)

// SQLStore is a Store implementation which utilizes a remote database having a
// "database/sql" compatible driver. For each consumer transaction, SQLStore
// begins a corresponding SQL transaction which may be obtained via Transaction,
// through which reads and writes to the store should be issued. A table
// "gazette_checkpoints" is also required, to which consumer checkpoints are
// persisted, and having a schema like:
//
//	CREATE TABLE gazette_checkpoints (
//	  shard_fqn  TEXT    PRIMARY KEY NOT NULL,
//	  fence      INTEGER NOT NULL,
//	  checkpoint BLOB    NOT NULL
//	);
//
// The user is responsible for creating this table. Exact data types may vary
// with the store dialect, but "shard_fqn" must be its PRIMARY KEY. StartCommit
// writes the checkpoint to this table before committing the transaction.
type SQLStore struct {
	DB *sql.DB

	// Cache is a provided for application use in the temporary storage of
	// in-memory state associated with a SQLStore. Eg, Cache might store records
	// which have been read this transaction and modified in-memory, and which
	// will be written out during FinalizeTxn.
	//
	// The representation of Cache is up to the application; it is not directly
	// used by SQLStore.
	Cache interface{}

	fence int64   // fence value after restore, which is verified each commit.
	table string  // checkpoint table name.
	txn   *sql.Tx // Current consumer transaction.
}

// NewSQLStore returns a new SQLStore using the *DB.
func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{
		DB:    db,
		table: "gazette_checkpoints",
		txn:   nil,
	}
}

// Transaction returns or (if not already begun) begins a SQL transaction.
// Optional *TxOptions have an effect only if a transaction has not already
// been started.
func (s *SQLStore) Transaction(ctx context.Context, txOpts *sql.TxOptions) (_ *sql.Tx, err error) {
	if s.txn == nil {
		s.txn, err = s.DB.BeginTx(ctx, txOpts)
	}
	return s.txn, err
}

// RestoreCheckpoint issues a SQL transaction which SELECTS the most recent
// Checkpoint of this shard FQN and also increments its "fence" column.
func (s *SQLStore) RestoreCheckpoint(shard Shard) (cp pc.Checkpoint, _ error) {
	var b []byte
	var txn, err = s.Transaction(shard.Context(), nil)

	if err == nil {
		_, err = txn.Exec(fmt.Sprintf("UPDATE %s SET fence=fence+1 "+
			"WHERE shard_fqn=$1;", s.table), shard.FQN())
	}
	if err == nil {
		err = txn.QueryRow(fmt.Sprintf("SELECT fence, checkpoint FROM %s "+
			"WHERE shard_fqn=$1;", s.table), shard.FQN()).Scan(&s.fence, &b)
	}
	if err == sql.ErrNoRows {
		err = nil // Leave |cp| as zero-valued.
	} else if err == nil {
		err = cp.Unmarshal(b)
	}

	if err == nil {
		err = s.StartCommit(shard, cp, nil).Err()
	}
	return cp, err
}

// StartCommit starts a background commit of the current transaction. While it
// runs, a new transaction may be started. The returned OpFuture will fail if
// the "fence" column has been further modified from the result previously read
// and updated by RestoreCheckpoint.
func (s *SQLStore) StartCommit(shard Shard, cp pc.Checkpoint, waitFor OpFutures) OpFuture {
	var b, err = cp.Marshal()
	if err != nil {
		return client.FinishedOperation(err)
	}
	txn, err := s.Transaction(shard.Context(), nil)
	if err != nil {
		return client.FinishedOperation(err)
	}
	var result = client.NewAsyncOperation()

	go func(txn *sql.Tx) {
		for op := range waitFor {
			if op.Err() != nil {
				result.Resolve(errors.WithMessage(op.Err(), "dependency failed"))
				return
			}
		}

		var update sql.Result
		var rowsAffected int64

		if s.fence == 0 {
			update, err = txn.Exec(fmt.Sprintf(`
				INSERT INTO %s (shard_fqn, checkpoint, fence) VALUES ($1, $2, 1);`, s.table), shard.FQN(), b)
			s.fence = 1
		} else {
			update, err = txn.Exec(fmt.Sprintf(`
				UPDATE %s SET checkpoint=$1 WHERE shard_fqn=$2 AND fence=$3;`, s.table), b, shard.FQN(), s.fence)
		}
		if err == nil {
			if rowsAffected, err = update.RowsAffected(); err == nil && rowsAffected == 0 {
				err = errors.Errorf("checkpoint fence was updated (ie, by a new primary)")
			}
		}
		if err == nil {
			err = txn.Commit()
		}
		result.Resolve(err)
	}(txn)
	s.txn = nil

	return result
}

// Destroy rolls back an existing transaction, but does not close the *DB
// instance previously passed to NewSQLStore.
func (s *SQLStore) Destroy() {
	if s.txn != nil {
		_ = s.txn.Rollback()
	}
}
