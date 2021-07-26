package store_sqlite

/*
#cgo LDFLAGS: -lsqlite3 -lrocksdb

#include "store.h"
*/
import "C"
import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/jgraettinger/gorocksdb"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	store_rocksdb "go.gazette.dev/core/consumer/store-rocksdb"
)

// Store is a consumer.Store implementation which wraps a primary SQLite database,
// as well as multiple auxiliary ATTACH'd databases. All file mutations of the
// database are sequenced into a recoverylog.Recorder for fault tolerance.
type Store struct {
	// PageDBOptions are options of the RocksDB which backs SQLite DBs.
	// NewStore initializes PageDBOptions with reasonable defaults, however
	// clients may wish to further customize.
	PageDBOptions *gorocksdb.Options
	// PageDBColumnOptions are column family RocksDB options applied to each
	// column family which backs a SQLite DB. NewStore initializes
	// PageDBColumnOptions with reasonable defaults, however clients may wish
	// to further customize.
	PageDBColumnOptions *gorocksdb.Options
	// PageDBColumns are names of RocksDB column families which back SQLite DBs,
	// and their associated *ColumnFamilyHandle.
	//
	// Typically only one SQLite DB is used, but additional DBs may also be
	// created via the SQLite "ATTACH" command, and each one is represented as
	// a RocksDB column family named after the DB (relative to the Recorder root).
	// One additional "default" family also exists, but is not used. Note attached
	// DBs must use this Store's Sqlite VFS (see also: URIForDB).
	// PageDBColumns is populated by NewStore.
	PageDBColumns map[string]*gorocksdb.ColumnFamilyHandle
	// PageDBEnv is the recorded RocksDB environment of the RocksDB
	// backing SQLite DB pages. NewStore initializes PageDBEnv.
	PageDBEnv *gorocksdb.Env
	// SQLiteURIValues of the filename URI used to open the primary SQLite
	// database. NewStore initializes SQLiteURIValues, but clients may wish to
	// customize URI parameters passed to the database prior to Open being
	// called, eg with:
	//
	// * "cache=shared" to enable SQLite's shared-cache mode, typically in
	//   combination with either "PRAGMA read_uncommitted" or go-sqlite3
	//   compiled with the "sqlite_unlock_notify" build tag (otherwise
	//   shared-cache mode increases the likelihood of "database is locked"
	//   errors; see https://github.com/mattn/go-sqlite3/issues/632).
	// * "_journal_mode=WAL" to enable SQLite WAL mode.
	//
	// See also the set of URI parameters supported by github.com/mattn/go-sqlite3`
	// and by SQLite itself (https://www.sqlite.org/uri.html). The "vfs" parameter
	// is specific to this particular Store, and must not be modified.
	SQLiteURIValues url.Values
	// SQLiteDB is the opened SQLite primary database. Clients may wish to use
	// this instance to perform read-only queries against the database outside
	// of consumer transactions. However, all mutations of the database should
	// be made through the store's Transaction. SQLiteDB is not set until Open.
	SQLiteDB *sql.DB
	// Cache is provided for application use in the temporary storage of
	// in-memory state associated with a Store. Eg, Cache might store records
	// which have been read and modified this transaction, and which will be
	// written out during FinalizeTxn.
	//
	// The representation of Cache is up to the application; it is not directly
	// used by Store.
	Cache interface{}
	// SQL statements prepared against the SQLiteDB (or a transaction thereof),
	// in the order provided to Open.
	Stmts []*sql.Stmt

	pages    *gorocksdb.DB         // RocksDB of SQLite DB pages.
	recorder *recoverylog.Recorder // Recorder of store mutations.
	vfs      *C.sqlite3_vfs        // SQLite VFS which hooks back into this Store.
	txn      *sql.Tx               // Current consumer transaction.
}

// NewStore builds a new Store instance around the Recorder.
// The caller must Open the returned Store before use.
func NewStore(recorder *recoverylog.Recorder) (*Store, error) {
	var s = &Store{
		PageDBOptions:       gorocksdb.NewDefaultOptions(),
		PageDBColumnOptions: gorocksdb.NewDefaultOptions(),
		PageDBColumns:       map[string]*gorocksdb.ColumnFamilyHandle{"default": nil},
		PageDBEnv:           store_rocksdb.NewHookedEnv(store_rocksdb.NewRecorder(recorder)),
		SQLiteURIValues: url.Values{
			"vfs":            {fmt.Sprintf("store-%d", time.Now().UnixNano())},
			"_synchronous":   {"FULL"},
			"_secure_delete": {"FAST"},
		},
		recorder: recorder,
	}

	s.PageDBOptions.SetMaxManifestFileSize(1 << 17) // 131,072 bytes.
	s.PageDBOptions.SetAllowMmapReads(true)
	s.PageDBOptions.SetAllowConcurrentMemtableWrites(false) // Required for in-place updates.

	s.PageDBColumnOptions.OptimizeForPointLookup(32)
	s.PageDBColumnOptions.SetInplaceUpdateSupport(true)
	s.PageDBColumnOptions.SetInplaceUpdateNumLocks(32) // We have a single writer & need few locks.

	// Set journal_mode depending on whether SQLite batch atomic writes are supported.
	if opt, err := SQLiteCompiledOptions(); err != nil {
		return nil, err
	} else if _, ok := opt["ENABLE_BATCH_ATOMIC_WRITE"]; ok {
		// In most cases, a rollback journal is not used at all. For remaining cases
		// where one is required, TRUNCATE results in the fewest CGO calls.
		s.SQLiteURIValues.Add("_journal_mode", "TRUNCATE")
	} else {
		s.SQLiteURIValues.Add("_journal_mode", "WAL")
	}

	// Query for existing column families of the pageDB.
	if _, err := os.Stat(s.pageDBPath()); os.IsNotExist(err) {
		s.PageDBOptions.SetCreateIfMissing(true)
	} else if err != nil {
		return nil, errors.WithMessage(err, "stat of pageDB path")
	} else if columns, err := gorocksdb.ListColumnFamilies(s.PageDBOptions, s.pageDBPath()); err != nil {
		return nil, errors.WithMessage(err, "listing pageDB column families")
	} else {
		for _, c := range columns {
			s.PageDBColumns[c] = nil
		}
	}

	// Wire this Store instance to be wrapped by a registered SQLite shim VFS,
	s.vfs = C.newRecFS(
		C.CString(s.SQLiteURIValues.Get("vfs")),
		C.CString(s.recorder.Dir()+string(filepath.Separator)),
	)
	if rc := sqlite3.ErrNo(C.sqlite3_vfs_register(s.vfs, 0)); rc != 0 {
		C.recFSFree(s.vfs)
		return nil, errors.WithMessage(rc, "registering SQLite VFS")
	}

	liveVFSs.mu.Lock()
	liveVFSs.m[uintptr(unsafe.Pointer(s.vfs))] = s
	liveVFSs.mu.Unlock()

	return s, nil
}

// Open the Store. The provided bootstrapSQL is executed against the DB
// before any other action is taken (this is a good opportunity to set PRAGMAs,
// create tables & indexes if they don't exist, set triggers, etc).
// The "gazette_checkpoint" table is then created, and any provided statements
// are prepared and added to Store.Stmts in the same order as provided to Open.
//
//      store.Open(
//          // Create myTable if it doesn't yet exist:
//          `CREATE TABLE IF NOT EXISTS myTable (
//              id BLOB PRIMARY KEY NOT NULL,
//              valueOne  INTEGER,
//              valueTwo  TEXT
//          );`,
//          // Statement for querying on valueOne:
//          `SELECT id, valueTwo FROM myTable WHERE valueOne > ?;`,
//          // Statement for upserting into myTable:
//          `INSERT INTO myTable(id, valueOne, valueTwo) VALUES(:id, :one, :two)
//              ON CONFLICT(id) DO UPDATE SET valueOne = valueOne + :one, valueTwo = :two`,
//      )
//      // store.Stmts[0] is now prepared for queries, and store.Stmts[1] for upserts.
//
func (s *Store) Open(bootstrapSQL string, statements ...string) error {
	// Finish initialization and open page DB.
	s.PageDBOptions.SetEnv(s.PageDBEnv)
	var cfOpts = make([]*gorocksdb.Options, 0, len(s.PageDBColumns))
	for _ = range s.PageDBColumns {
		cfOpts = append(cfOpts, s.PageDBColumnOptions)
	}

	// Open the PageDB and collect column family handles.
	var columns = make([]string, 0, len(s.PageDBColumns))
	var handles gorocksdb.ColumnFamilyHandles
	var err error

	for n := range s.PageDBColumns {
		columns = append(columns, n)
	}
	if s.pages, handles, err = gorocksdb.OpenDbColumnFamilies(
		s.PageDBOptions, s.pageDBPath(), columns, cfOpts); err != nil {
		return errors.WithMessage(err, "opening page-file RocksDB")
	}
	for i := range columns {
		s.PageDBColumns[columns[i]] = handles[i]
	}

	// Open and bootstrap the primary SQLite database.
	if s.SQLiteDB, err = sql.Open("sqlite3", s.URIForDB("primary.db")); err != nil {
		return errors.WithMessage(err, "opening primary SQLite DB")
	}
	if _, err = s.SQLiteDB.Exec(bootstrapSQL); err != nil {
		return errors.WithMessage(err, "executing user-provided bootstrap")
	}
	if _, err = s.SQLiteDB.Exec(`
		CREATE TABLE IF NOT EXISTS gazette_checkpoint (
			rowid INTEGER PRIMARY KEY DEFAULT 0 CHECK (rowid = 0), -- Permit just one row.
			checkpoint BLOB
		);
		INSERT OR IGNORE INTO gazette_checkpoint(rowid, checkpoint) VALUES (0, '');
	`); err != nil {
		return errors.WithMessage(err, "checkpoint table bootstrap")
	}
	for _, stmtStr := range statements {
		if stmt, err := s.SQLiteDB.Prepare(stmtStr); err != nil {
			return errors.WithMessagef(err, "preparing statement %q", stmtStr)
		} else {
			s.Stmts = append(s.Stmts, stmt)
		}
	}

	// This package's VFS implementation assumes only one connection will be
	// using it at a time. If this assumption is violated, it can result in
	// "fnode not tracked" recorder FSM errors, because the FNode associated
	// with an open transaction log recFS is updated in one connection and not
	// the other. This /could/ be fixed to be properly concurrent, but SQLite
	// (and mattn/go-sqlite3) don't handle concurrency particularly well anyway.
	//
	// Default to allowing only one database connection, as a sane and safe option
	// that's probably fast enough for your use case. If desired this can be
	// increased so long as shared-cache mode is also enabled, such that all open
	// connections share a common VFS instance. Note this also requires building
	// go-sqlite3 with the 'sqlite_unlock_notify' build tag.
	s.SQLiteDB.SetMaxOpenConns(1)

	return nil
}

// URIForDB takes a SQLite database name and returns a suitable SQLite
// URI given the Store's current SQLiteURIValues. Use URIForDB to map a relative
// database name (which is held constant across Store recoveries) to a URI
// specific to this *Store instance and which is suited for ATTACH-ing to the
// primary database. Eg:
//   URIForDB("sept_2019.db") =>
//     "file:sept_2019.db?_journal_mode=WAL&other-setting=bar&vfs=store-123456&..."
func (s *Store) URIForDB(name string) string {
	return "file:" + name + "?" + s.SQLiteURIValues.Encode()
}

// Transaction returns or (if not already begun) begins a SQL transaction.
// Optional *TxOptions have an effect only if Transaction begins a new
// SQL transaction.
func (s *Store) Transaction(ctx context.Context, txOpts *sql.TxOptions) (_ *sql.Tx, err error) {
	if s.txn == nil {
		s.txn, err = s.SQLiteDB.BeginTx(ctx, txOpts)
	}
	return s.txn, err
}

// RestoreCheckpoint SELECTS the most recent Checkpoint of this Store.
func (s *Store) RestoreCheckpoint(_ consumer.Shard) (cp pc.Checkpoint, _ error) {
	var b []byte
	var txn, err = s.Transaction(context.Background(), nil)

	if err == nil {
		err = txn.QueryRow("SELECT checkpoint FROM gazette_checkpoint").Scan(&b)
	}
	if err == nil {
		err = cp.Unmarshal(b)
	}
	return cp, err
}

// StartCommit commits the current transaction to the local SQLite database, and returns
// a recorder barrier which resolves once all mutations of the database have been persisted
// to the recovery log.
func (s *Store) StartCommit(_ consumer.Shard, cp pc.Checkpoint, waitFor client.OpFutures) client.OpFuture {
	_ = s.recorder.Barrier(waitFor)

	// Marshal checkpoint, add it to the transaction, and locally commit the transaction.
	var txn *sql.Tx
	var b, err = cp.Marshal()

	if err == nil {
		txn, err = s.Transaction(context.Background(), nil)
		s.txn = nil
	}
	if err == nil {
		_, err = txn.Exec(`UPDATE gazette_checkpoint SET checkpoint = ?;`, b)
	}
	if err == nil {
		err = txn.Commit()
	}
	if err == nil {
		// The local commit we just did drove a bunch of local SQLite writes which
		// are queued behind |waitFor| and not yet written to the recovery log.
		// The Store commit has only occurred after all of these queued writes
		// commit to Gazette.
		return s.recorder.Barrier(nil)
	}
	return client.FinishedOperation(err)
}

// Destroy the Store, removing the local processing directory and freeing
// associated resources.
func (s *Store) Destroy() {
	for _, s := range s.Stmts {
		if err := s.Close(); err != nil {
			log.WithField("err", err).Error("failed to close SQLite Stmt")
		}
	}
	if s.SQLiteDB != nil {
		if err := s.SQLiteDB.Close(); err != nil {
			log.WithFields(log.Fields{
				"dir": s.recorder.Dir(),
				"err": err,
			}).Error("failed to close SQLite DB")
		}
	}
	if s.vfs != nil {
		if rc := sqlite3.ErrNo(C.sqlite3_vfs_unregister(s.vfs)); rc != 0 {
			log.WithFields(log.Fields{
				"dir": s.recorder.Dir(),
				"err": rc.Error(),
			}).Error("failed to unregister VFS shim")
		}
		C.recFSFree(s.vfs)
	}

	if s.pages != nil {
		s.pages.Close()
	}
	s.PageDBColumnOptions.Destroy()
	s.PageDBOptions.Destroy()
	s.PageDBEnv.Destroy()

	if err := os.RemoveAll(s.recorder.Dir()); err != nil {
		log.WithFields(log.Fields{
			"dir": s.recorder.Dir(),
			"err": err,
		}).Error("failed to remove store directory")
	}
}

func (s *Store) pageDBPath() string {
	return filepath.Join(s.recorder.Dir(), "pageDB")
}

// SQLiteCompiledOptions returns the set of compile-time options that
// the linked SQLite library was built with. See https://www.sqlite.org/compile.html
// for a full listing. Note the "SQLITE_" prefix is dropped in the returned set:
//
//      map[string]struct{
//          "COMPILER=gcc-8.3.0": {},
//          "ENABLE_BATCH_ATOMIC_WRITE": {},
//          "ENABLE_COLUMN_METADATA": {},
//          "ENABLE_DBSTAT_VTAB": {},
//          ... etc ...
//      }
//
func SQLiteCompiledOptions() (map[string]struct{}, error) {
	var db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA compile_options;")
	if err != nil {
		return nil, err
	}

	var m = make(map[string]struct{})
	for rows.Next() {
		var opt string
		if err = rows.Scan(&opt); err != nil {
			return nil, err
		}
		m[opt] = struct{}{}
	}
	return m, rows.Err()
}

//export cgoOpenPageFile
func cgoOpenPageFile(vfs *C.sqlite3_vfs, cPath *C.char, f *C.sqlite3_file) C.int {
	var store = vfsToStore(vfs)

	var name = C.GoString(cPath)
	if !strings.HasPrefix(name, store.recorder.Dir()) {
		log.WithFields(log.Fields{
			"dir":  store.recorder.Dir(),
			"path": name,
		}).Error("DB path is not rooted by recorder.Dir")
		return C.int(sqlite3.ErrCantOpen)
	}
	name = path.Clean(filepath.ToSlash(name[len(store.recorder.Dir()):]))

	// Find a column family corresponding to |name|. If one doesn't exist, create it.
	var handle, ok = store.PageDBColumns[name]
	if !ok {
		var err error
		handle, err = store.pages.CreateColumnFamily(store.PageDBColumnOptions, name)
		if err != nil {
			log.WithFields(log.Fields{
				"err":  err,
				"name": name,
			}).Error("failed to create page DB column family")
			return C.int(sqlite3.ErrCantOpen)
		}
		store.PageDBColumns[name] = handle
	}

	C.configureAsPageFile(f,
		(*gorocksdbDB)(unsafe.Pointer(store.pages)).c,
		(*gorocksdbColumnFamilyHandle)(unsafe.Pointer(handle)).c,
		fileBufferSize,
	)
	return sqlite3.SQLITE_OK
}

//export cgoOpenLogFile
func cgoOpenLogFile(vfs *C.sqlite3_vfs, cPath *C.char, f *C.sqlite3_file, cFlags C.int) C.int {
	var path = C.GoString(cPath)

	var flags int
	if cFlags&kSqliteOpenExclusive != 0 {
		flags |= os.O_EXCL
	}
	if cFlags&kSqliteOpenReadwrite != 0 {
		flags |= os.O_RDWR
	}
	if cFlags&kSqliteOpenCreate != 0 {
		flags |= os.O_CREATE
	}

	C.configureAsLogFile(f,
		C.uintptr_t(uintptr(unsafe.Pointer(vfs))),
		C.int64_t(vfsToStore(vfs).recorder.RecordOpenFile(path, flags)),
		fileBufferSize,
	)
	return sqlite3.SQLITE_OK
}

//export cgoDelete
func cgoDelete(vfs *C.sqlite3_vfs, cPath *C.char) C.int {
	vfsToStore(vfs).recorder.RecordRemove(C.GoString(cPath))
	return sqlite3.SQLITE_OK
}

//export cgoLogFileWrite
func cgoLogFileWrite(vfs *C.sqlite3_vfs, fnode *C.int64_t, cPath *C.char, b unsafe.Pointer, n C.int, offset C.int64_t) int {
	var store = vfsToStore(vfs)

	// Initialize |data| to the underlying |raw| array, without a copy.
	var data []byte
	var header = (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data, header.Len, header.Cap = uintptr(b), int(n), int(n)

	if offset == 0 && len(data) >= 4 {

		switch v := binary.BigEndian.Uint32(data); v {
		case 0x00000000, kWALHeaderLittleEndian, kWALHeaderBigEndian:
			// These are cases where a transaction log is being semantically
			// restarted (even though SQLite keeps the file itself around so
			// as not to churn the file system). We handle by recording a
			// truncation, which allows the previous content to be dropped from
			// the Recorder's live-file manifest.
			//
			// 0x00000000: Write which scribbles to & invalidates a rollback
			// journal header (see PRAGMA journal_mode=PERSIST).
			//
			// 0x377f068x: Write which re-starts a WAL. SQLite keeps WAL and
			// journal files around indefinitely, but semantically they're
			// append-only when invalidated and/or restarted.
			var path = C.GoString(cPath)
			*fnode = C.int64_t(store.recorder.RecordOpenFile(path, os.O_RDWR|os.O_TRUNC))

		case kHotJournalHeader:
			// Write to header of a hot rollback journal.

		default:
			// This is likely a master journal of a multi-DB transaction.
			// Such journals don't have a fixed header (they're currently
			// formatted as "db-name-one.db\x00db-name-two.db\x00" etc).
		}
	}

	store.recorder.RecordWriteAt(recoverylog.Fnode(*fnode), data, int64(offset))
	return sqlite3.SQLITE_OK
}

//export cgoLogFileTruncate
func cgoLogFileTruncate(vfs *C.sqlite3_vfs, fnode *C.int64_t, cPath *C.char, size C.int64_t) int {
	if size != 0 {
		return 0
	}

	*fnode = C.int64_t(vfsToStore(vfs).recorder.
		RecordOpenFile(C.GoString(cPath), os.O_RDWR|os.O_TRUNC))
	return sqlite3.SQLITE_OK
}

// liveVFSs holds registered VFS shims and their associated *Store instances.
var liveVFSs = struct {
	m  map[uintptr]*Store
	mu sync.Mutex
}{m: make(map[uintptr]*Store)}

func vfsToStore(vfs *C.sqlite3_vfs) *Store {
	liveVFSs.mu.Lock()
	var store = liveVFSs.m[uintptr(unsafe.Pointer(vfs))]
	liveVFSs.mu.Unlock()
	return store
}

// Select SQLITE flags which we use.
const (
	kSqliteOpenReadwrite   = 0x00000002
	kSqliteOpenCreate      = 0x00000004
	kSqliteOpenExclusive   = 0x00000010
	kSqliteOpenMainDB      = 0x00000100
	kSqliteOpenMainJournal = 0x00000800

	kSqliteFcntlBeginAtomicWrite    = 31
	kSqliteFcntlCommitAtomicWrite   = 32
	kSqliteFcntlRollbackAtomicWrite = 33

	kHotJournalHeader      = 0xd9d505f9
	kWALHeaderLittleEndian = 0x377f0682
	kWALHeaderBigEndian    = 0x377f0683
)

// fileBufferSize is the amount of buffering page and transaction log files
// will employ before flushing to CGO hooks. This greatly reduces the number
// of CGO calls made due to database mutations.
var fileBufferSize C.ulong = 1 << 17 // 128KB.

type gorocksdbDB struct {
	c    *C.rocksdb_t
	name string
	opts *gorocksdb.Options
}

type gorocksdbColumnFamilyHandle struct {
	c *C.rocksdb_column_family_handle_t
}

func init() {
	if unsafe.Sizeof(new(gorocksdb.DB)) != unsafe.Sizeof(new(gorocksdbDB)) ||
		unsafe.Alignof(new(gorocksdb.DB)) != unsafe.Alignof(new(gorocksdbDB)) {
		panic("did gorocksdb.DB change? store-sqlite cannot safely reinterpret-cast")
	}
	if unsafe.Sizeof(new(gorocksdb.ColumnFamilyHandle)) != unsafe.Sizeof(new(gorocksdbColumnFamilyHandle)) ||
		unsafe.Alignof(new(gorocksdb.ColumnFamilyHandle)) != unsafe.Alignof(new(gorocksdbColumnFamilyHandle)) {
		panic("did gorocksdb.ColumnFamilyHandle change? store-sqlite cannot safely reinterpret-cast")
	}
}
