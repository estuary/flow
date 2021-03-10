package sql

import (
	"context"
	"encoding/base64"
	"fmt"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Fence is an installed barrier in a shared checkpoints table which prevents
// other sessions from committing transactions under the fenced ID --
// and prevents this Fence from committing where another session has in turn
// fenced this instance off.
type Fence struct {
	// Checkpoint associated with this Fence.
	Checkpoint []byte

	// fence is the current value of the monotonically increasing integer used to identify unique
	// instances of transactions rpcs.
	fence int64
	// shardFQN is the fully qualified id of the materialization shard.
	shardFQN  string
	ctx       context.Context
	updateSQL string
}

// LogEntry returns a log.Entry with pre-set fields that identify the Shard ID and Fence
func (f *Fence) LogEntry() *log.Entry {
	return log.WithFields(log.Fields{
		"fqn":   f.shardFQN,
		"fence": f.fence,
	})
}

// NewFence installs and returns a new *Fence. On return, all older fences of
// this |shardFqn| have been fenced off from committing further transactions.
func (e *Endpoint) NewFence(shardFqn string) (*Fence, error) {
	var txn, err = e.DB.BeginTx(e.Context, nil)
	if err != nil {
		return nil, fmt.Errorf("db.BeginTx: %w", err)
	}

	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	// Attempt to increment the fence value.
	var rowsAffected int64
	if result, err := txn.Exec(
		fmt.Sprintf(
			"UPDATE %s SET fence=fence+1 WHERE shard_fqn=%s;",
			e.Tables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
		),
		shardFqn,
	); err != nil {
		return nil, fmt.Errorf("incrementing fence: %w", err)
	} else if rowsAffected, err = result.RowsAffected(); err != nil {
		return nil, fmt.Errorf("result.RowsAffected: %w", err)
	}

	// If the fence doesn't exist, insert it now.
	if rowsAffected != 0 {
		// Exists; no-op.
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			"INSERT INTO %s (shard_fqn, checkpoint, fence) VALUES (%s, %s, 1);",
			e.Tables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
		),
		shardFqn,
		// Initialize a checkpoint such that the materialization starts from
		// scratch, regardless of the runtime's internal checkpoint.
		base64.StdEncoding.EncodeToString(pm.ExplicitZeroCheckpoint),
	); err != nil {
		return nil, fmt.Errorf("inserting fence: %w", err)
	}

	// Read the just-incremented fence value, and the last-committed checkpoint.
	var fence int64
	var checkpointB64 string

	if err = txn.QueryRow(
		fmt.Sprintf(
			"SELECT fence, checkpoint FROM %s WHERE shard_fqn=%s;",
			e.Tables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
		),
		shardFqn,
	).Scan(&fence, &checkpointB64); err != nil {
		return nil, fmt.Errorf("scanning fence and checkpoint: %w", err)
	}

	checkpoint, err := base64.StdEncoding.DecodeString(checkpointB64)
	if err != nil {
		return nil, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	err = txn.Commit()
	txn = nil // Disable deferred rollback.

	if err != nil {
		return nil, fmt.Errorf("txn.Commit: %w", err)
	}

	// Craft SQL which is used for future commits under this fence.
	var updateSQL = fmt.Sprintf(
		"UPDATE %s SET checkpoint=%s WHERE shard_fqn=%s AND fence=%s;",
		e.Tables.Checkpoints.Identifier,
		e.Generator.Placeholder(0),
		e.Generator.Placeholder(1),
		e.Generator.Placeholder(2),
	)

	return &Fence{
		Checkpoint: checkpoint,
		ctx:        e.Context,
		fence:      fence,
		shardFQN:   shardFqn,
		updateSQL:  updateSQL,
	}, nil
}

// Update the fence and its Checkpoint, returning an error if this Fence
// has in turn been fenced off by another.
// Update takes a ExecFn callback which should be scoped to a database transaction,
// such as sql.Tx or a database-specific transaction implementation.
func (f *Fence) Update(execFn ExecFn) error {
	rowsAffected, err := execFn(
		f.ctx,
		f.updateSQL,
		base64.StdEncoding.EncodeToString(f.Checkpoint),
		f.shardFQN,
		f.fence,
	)
	if err == nil && rowsAffected == 0 {
		err = errors.Errorf("this transactions session was fenced off by another")
	}
	return err
}

// ExecFn executes a |sql| statement with |arguments|, and returns the number of rows affected.
type ExecFn func(ctx context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error)
