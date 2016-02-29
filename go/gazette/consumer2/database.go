package consumer

import (
	rocks "github.com/tecbot/gorocksdb"

	"github.com/pippio/gazette/journal"
	"github.com/pippio/gazette/recoverylog"
)

type database struct {
	recoveryLog journal.Name
	logWriter   journal.Writer
	recorder    *recoverylog.Recorder

	*rocks.DB
	env          *rocks.Env
	options      *rocks.Options
	writeOptions *rocks.WriteOptions
	readOptions  *rocks.ReadOptions
	writeBatch   *rocks.WriteBatch
}

func newDatabase(fsm *recoverylog.FSM, dir string, writer journal.Writer) (*database, error) {
	recorder, err := recoverylog.NewRecorder(fsm, len(dir), writer)
	if err != nil {
		return nil, err
	}

	db := &database{
		recoveryLog: fsm.LogMark.Journal,
		logWriter:   writer,
		recorder:    recorder,

		env:          rocks.NewObservedEnv(recorder),
		options:      rocks.NewDefaultOptions(),
		readOptions:  rocks.NewDefaultReadOptions(),
		writeOptions: rocks.NewDefaultWriteOptions(),
		writeBatch:   rocks.NewWriteBatch(),
	}

	db.options.SetEnv(db.env)
	db.options.SetCreateIfMissing(true)

	// By default, we instruct RocksDB *not* to perform data syncs. We already
	// capture linearization of file write/rename/link operations via Gazette,
	// and transactions are applied via an atomic write batch. The result is
	// that we'll always recover a consistent database.
	//
	// Note that the consumer loop also installs a write-barrier between
	// transactions, which will block a current transaction from committing
	// until the previous one has been fully synced by Gazette.
	db.options.SetDisableDataSync(true)

	db.DB, err = rocks.OpenDb(db.options, dir)
	if err != nil {
		return db, err
	}
	return db, nil
}

func (db *database) commit() (<-chan struct{}, error) {
	if err := db.Write(db.writeOptions, db.writeBatch); err != nil {
		return nil, err
	}
	db.writeBatch.Clear()

	// Issue an empty write. As writes from a client to a journal are applied
	// strictly in order, this is effectively a commit barrier: when it resolves,
	// the client knows the commit has been fully synced by Gazette.
	return db.logWriter.Write(db.recoveryLog, nil)
}

func (db *database) teardown() {
	if db.DB != nil {
		// Blocks until all background compaction has completed.
		db.DB.Close()
		db.DB = nil
	}
	if db.env != nil {
		db.env.Destroy()
		db.env = nil
	}

	db.options.Destroy()
	db.readOptions.Destroy()
	db.writeOptions.Destroy()
	db.writeBatch.Destroy()
}
