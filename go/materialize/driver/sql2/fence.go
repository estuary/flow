package sql

import (
	"context"
	"database/sql"
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
	// Full name of the fenced materialization.
	materialization string
	// [keyBegin, keyEnd) identify the range of keys covered by this Fence.
	keyBegin uint32
	keyEnd   uint32

	ctx       context.Context
	updateSQL string
}

// LogEntry returns a log.Entry with pre-set fields that identify the Shard ID and Fence
func (f *Fence) LogEntry() *log.Entry {
	return log.WithFields(log.Fields{
		"materialization": f.materialization,
		"keyBegin":        f.keyBegin,
		"keyEnd":          f.keyEnd,
		"fence":           f.fence,
	})
}

// NewFence installs and returns a new *Fence. On return, all older fences of
// this |shardFqn| have been fenced off from committing further transactions.
func (e *Endpoint) NewFence(materialization string, keyBegin, keyEnd uint32) (*Fence, error) {
	var txn, err = e.DB.BeginTx(e.Context, nil)
	if err != nil {
		return nil, fmt.Errorf("db.BeginTx: %w", err)
	}

	defer func() {
		if txn != nil {
			txn.Rollback()
		}
	}()

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
	if _, err = txn.Exec(
		fmt.Sprintf(`
			UPDATE %s
				SET fence=fence+1
				WHERE materialization=%s
				AND key_end>=%s
				AND key_begin<=%s
			;
			`,
			e.Tables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
		),
		materialization,
		keyBegin,
		keyEnd,
	); err != nil {
		return nil, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end]
	// which fully overlaps our range.
	var fence int64
	var readBegin, readEnd uint32
	var checkpointB64 string

	if err = txn.QueryRow(
		fmt.Sprintf(`
			SELECT fence, key_begin, key_end, checkpoint
				FROM %s
				WHERE materialization=%s
				AND key_begin<=%s
				AND key_end>=%s
				ORDER BY key_end - key_begin ASC
				LIMIT 1
			;
			`,
			e.Tables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
		),
		materialization,
		keyBegin,
		keyEnd,
	).Scan(&fence, &readBegin, &readEnd, &checkpointB64); err == sql.ErrNoRows {
		// A checkpoint doesn't exist. Use an implicit checkpoint value.
		fence = 1
		// Initialize a checkpoint such that the materialization starts from
		// scratch, regardless of the runtime's internal checkpoint.
		checkpointB64 = base64.StdEncoding.EncodeToString(pm.ExplicitZeroCheckpoint)
		// Set an invalid range, which compares as unequal to trigger an insertion below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return nil, fmt.Errorf("scanning fence and checkpoint: %w", err)
	}

	// If a checkpoint for this exact range doesn't exist, insert it now.
	if readBegin == keyBegin && readEnd == keyEnd {
		// Exists; no-op.
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			"INSERT INTO %s (materialization, key_begin, key_end, checkpoint, fence) VALUES (%s, %s, %s, %s, %s);",
			e.Tables.Checkpoints.Identifier,
			e.Generator.Placeholder(0),
			e.Generator.Placeholder(1),
			e.Generator.Placeholder(2),
			e.Generator.Placeholder(3),
			e.Generator.Placeholder(4),
		),
		materialization,
		keyBegin,
		keyEnd,
		checkpointB64,
		fence,
	); err != nil {
		return nil, fmt.Errorf("inserting fence: %w", err)
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
		"UPDATE %s SET checkpoint=%s WHERE materialization=%s AND key_begin=%s AND key_end=%s AND fence=%s;",
		e.Tables.Checkpoints.Identifier,
		e.Generator.Placeholder(0),
		e.Generator.Placeholder(1),
		e.Generator.Placeholder(2),
		e.Generator.Placeholder(3),
		e.Generator.Placeholder(4),
	)

	return &Fence{
		Checkpoint:      checkpoint,
		ctx:             e.Context,
		fence:           fence,
		materialization: materialization,
		keyBegin:        keyBegin,
		keyEnd:          keyEnd,
		updateSQL:       updateSQL,
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
		f.materialization,
		f.keyBegin,
		f.keyEnd,
		f.fence,
	)
	if err == nil && rowsAffected == 0 {
		err = errors.Errorf("this transactions session was fenced off by another")
	}
	return err
}

// ExecFn executes a |sql| statement with |arguments|, and returns the number of rows affected.
type ExecFn func(ctx context.Context, sql string, arguments ...interface{}) (rowsAffected int64, _ error)
