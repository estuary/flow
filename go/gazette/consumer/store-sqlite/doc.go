// Package store_sqlite implements the consumer.Store interface via an embedded
// SQLite instance.
//
// # Database Representation
//
// SQLite records state across a very long lived database file, and short-lived
// transactional log files (journals, or write-ahead logs; see
// https://www.sqlite.org/fileformat.html).
//
// The central strategy of this package is to provide a SQLite VFS implementation
// (https://www.sqlite.org/vfs.html) with "hooks" to record mutations of these
// files made by the database, as they occur, into a recovery log. Recovering
// a database consists of restoring the on-disk representation of the main
// database and associated transaction logs.
//
// SQLite transaction logs are well-suited to this strategy, as they're append-
// only and (typically) short-lived. The main database file is not. It has
// unbounded lifetime and is read & written randomly by the SQLite engine.
// Directly representing the main database as a recoverylog.FNode would result
// in an unbounded recovery horizon, as every operation ever done against the
// database would need to be replayed. There must be a means of "compacting"
// away earlier recorded DB mutations which are no longer needed in order to
// tighten the recovery playback horizon to a more reasonable bound.
//
// SQLite does make certain guarantees with respect to the main database file,
// particularly that reads and writes are always page-aligned, and of page size.
// The database file can thus be thought of as an associative map of page
// offsets and their current content. This package therefore employs a RocksDB
// instance, recorded into the same recovery log, into which page offsets and
// values are written and read. This adds some overhead to page reads and writes,
// but provides a much tighter bound to the recovery playback horizon (since
// RocksDB compactions will eventually discard old versions of pages that have
// since been re-written).
//
// Stores may have multiple associated SQLite databases: a primary database,
// and arbitrary secondary databases ATTACH'd to the primary. Distinct databases
// are tracked as distinct column families of a single RocksDB.
//
// Transaction logs (SQLite journals & the WAL) are directly recorded into the
// recovery log. SQLite provides multiple "journal_mode" settings, such as
// PERSIST and TRUNCATE, which cause a log file to be re-used indefinitely:
// this implementation detects and maps these operations into new recorded
// logs, to ensure a tighter recovery playback horizon.
//
// # Batch Atomic Writes
//
// Where SQLITE_BATCH_ATOMIC_WRITE is enabled, the Store VFS presents itself
// as a supporting file system, with atomic write semantics implemented
// by a RocksDB WriteBatchWithIndex. This arrangement allows SQLite to
// by-pass writing a rollback journal altogether in almost all cases,
// and page mutations are written only once to the RocksDB WAL (reducing
// write amplification).
//
// For remaining cases where a rollback journal is required (eg, because
// the SQLite pager's dirty page cache spills to disk, or because the
// transaction involves multiple journals), the choice of _journal_mode
// doesn't much matter but TRUNCATE or PERSIST have fewer CGO calls.
//
// Note that _without_ BATCH_ATOMIC_WRITE mode, rollback journals result
// in substantial write amplification on a per-transaction basis, as each
// page is written first to the rollback journal (before its mutation)
// and also to the RocksDB WAL (after its mutation).
//
// For this reason if BATCH_ATOMIC_WRITE is unavailable, use of
// journal_mode=WAL is recommended, as each page mutation is then written
// only once during a transaction to the SQLite WAL (and not to RocksDB).
// Later, when a WAL checkpoint is executed, only the most-recent version
// of a page is then written to RocksDB (and its WAL).
//
// # Buffering and the Synchronous Pragma
//
// The Store VFS employs buffering in its C++ bindings to amortize and minimize
// the number of calls which must cross the CGO barrier (as these can be quite
// slow). It's completely reliant on the "sync" signals delivered by the SQLite
// engine to understand when buffered data must be delivered over CGO and
// sequenced into the recovery log. For this reason, the PRAGMA *must be*:
//
//	PRAGMA synchronous=FULL;
//
// The default SQLite URI flags used by the Store set this pragma. SQLite
// behavior for any lower setting, such as synchronous=NORMAL, is to not sync the
// SQLite write-ahead log after each transaction commit, which is incompatible
// with this implementation and the expectations of the Store interface
// (specifically, that after StartCommit all mutations of the transaction have
// been flushed to the recovery log).
//
// As Store databases are by nature ephemeral, and durability is provided by the
// recovery log, the Store VFS *does not* propagate sync signals to OS file
// delegates, and never waits for a host disk to sync. "synchronous=FULL" thus
// has negligible performance overhead as compared to "synchronous=OFF".
package store_sqlite
