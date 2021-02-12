package store_sqlite

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/tecbot/gorocksdb"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/brokertest"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/etcdtest"
)

func TestSimple(t *testing.T) {
	// A basic SQL sequence with crash-recovery.
	runSequenceTest(t, "testdata/test-simple.sql", nil)
}

func TestSQLLogic(t *testing.T) {
	// A comprehensive SQL logic test derived from SQLite's sqllogictest suite,
	// with multiple crash-recovery stages and deeply nested reads of DB pages.
	runSequenceTest(t, "testdata/test-sqllogic.sql", nil)
}

func TestAttachAndVacuum(t *testing.T) {
	// A test which exercises ATTACH-ing additional databases, writing
	// and vacuuming into them, re-attachment of DBs after crash recovery,
	// and multi-DB queries.
	runSequenceTest(t, "testdata/test-attach-and-vacuum.sql", nil)
}

func TestTrimAndRollback(t *testing.T) {
	// This sequence uses a small cache size with journaling to force SQLite
	// to stage changes within the database itself, and then undo those changes
	// on ROLLBACK.
	runSequenceTest(t, "testdata/test-trim-and-rollback.sql", nil)
}

func TestMultiDBTxn(t *testing.T) {
	// This test exercises basic multi-DB transactions and crash recovery.
	runSequenceTest(t, "testdata/test-multi-db-txn.sql", nil)
}

func TestStoreCommitAndRecover(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	var ajc = client.NewAppendService(context.Background(), bk.Client())
	var fsm, _ = recoverylog.NewFSM(recoverylog.FSMHints{Log: aRecoveryLog})
	var tmpdir, err = ioutil.TempDir("", "store-sqlite")
	require.NoError(t, err)

	var recorder = recoverylog.NewRecorder(
		aRecoveryLog,
		fsm,
		recoverylog.NewRandomAuthor(),
		tmpdir,
		ajc,
	)
	store, err := NewStore(recorder)
	require.NoError(t, err)
	require.NoError(t, store.Open(""))

	// New database => empty checkpoint.
	checkpoint, err := store.RestoreCheckpoint(nil)
	require.NoError(t, err)
	require.Equal(t, pc.Checkpoint{}, checkpoint)

	// Start a transaction, and write some data.
	txn, err := store.Transaction(context.Background(), nil)
	require.NoError(t, err)
	_, err = txn.Exec(`
		CREATE TABLE foo (bar TEXT);
		INSERT INTO foo(bar) VALUES ("bar!"), ("baz");
    `)
	require.NoError(t, err)

	// Begin a commit which won't finish until |waitFor| resolves.
	var waitFor = client.NewAsyncOperation()
	var commitOp = store.StartCommit(nil, pc.Checkpoint{
		Sources: map[pb.Journal]*pc.Checkpoint_Source{
			"journal/A": {ReadThrough: 1234},
		},
	}, client.OpFutures{waitFor: {}})

	// Expect our previous change is already visible in a new transaction.
	txn, err = store.Transaction(context.Background(), nil)
	require.NoError(t, err)

	var cnt int
	require.NoError(t, txn.QueryRow(`SELECT COUNT(*) FROM foo;`).Scan(&cnt))
	require.Equal(t, 2, cnt)

	checkpoint, err = store.RestoreCheckpoint(nil)
	require.NoError(t, err)
	require.Equal(t, pc.Checkpoint{
		Sources: map[pb.Journal]*pc.Checkpoint_Source{
			"journal/A": {ReadThrough: 1234},
		},
	}, checkpoint)

	// However the commit hasn't yet synced to the recovery log.
	select {
	case <-commitOp.Done():
		require.FailNow(t, "commitOp should still be running")
	default:
		// Pass.
	}

	// Resolve |waitFor|, and expect |commitOp| then resolves.
	waitFor.Resolve(nil)
	require.NoError(t, commitOp.Err())

	for op := range ajc.PendingExcept("") {
		<-op.Done()
	}
	store.Destroy()
	_, err = os.Stat(tmpdir)
	require.True(t, os.IsNotExist(err)) // Removed by Destroy.

	cleanup()
}

func TestParsingOfPageFileHeader(t *testing.T) {
	var b = makePageFileHeader(1024, 1337133713)
	var pageSize, pageCount, changeCtr, freeHead, freeCount = testParsePageFileHeader(b)

	require.Equal(t, pageSize, 1024)
	require.Equal(t, pageCount, 1337133713)
	require.Equal(t, changeCtr, 1111111111)
	require.Equal(t, freeHead, 2222222222)
	require.Equal(t, freeCount, 3333333333)

	// Handling of 65K special-case (encoded by SQLite as '1').
	b = makePageFileHeader(1, 12)
	pageSize, pageCount, _, _, _ = testParsePageFileHeader(b)

	require.Equal(t, pageSize, 1<<16)
	require.Equal(t, pageCount, 12)

	// Test with a fixture pulled from a SQLite DB.
	b = []byte("SQLite format 3\x00\x02\x00\x01\x01\x00@  \x00\x00\x00\x07" +
		"\x00\x00\x00\x90\x00\x00\x00$\x00\x00\x00\"\x00\x00\x00\x03\x00\x00" +
		"\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00.0:")
	pageSize, pageCount, changeCtr, freeHead, freeCount = testParsePageFileHeader(b)

	require.Equal(t, pageSize, 512)
	require.Equal(t, pageCount, 144)
	require.Equal(t, changeCtr, 7)
	require.Equal(t, freeHead, 36)
	require.Equal(t, freeCount, 34)
}

func TestFileBindingCases(t *testing.T) {
	var bk, cleanup = newBrokerAndLog(t)
	var ajc = client.NewAppendService(context.Background(), bk.Client())
	var fsm, _ = recoverylog.NewFSM(recoverylog.FSMHints{Log: aRecoveryLog})
	var tmpdir, err = ioutil.TempDir("", "store-sqlite")
	require.NoError(t, err)

	var recorder = recoverylog.NewRecorder(
		aRecoveryLog,
		fsm,
		recoverylog.NewRandomAuthor(),
		tmpdir,
		ajc,
	)
	store, err := NewStore(recorder)
	require.NoError(t, err)
	require.NoError(t, store.Open(""))

	// For this test, buffer up to 3.5x 512-byte pages.
	var restore = fileBufferSize
	defer func() { fileBufferSize = restore }()
	fileBufferSize = (7 * 512) / 2

	t.Run("pageReadAndWrite", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "page-rw.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainDB)
		require.Equal(t, 0, rc)

		var page = makePageFileHeader(512, 2)
		require.Equal(t, 0, testPageFileWrite(f, page, 0))

		// Expect size reflects the header's page count & size.
		size, rc := testPageFileSize(f)
		require.Equal(t, 0, rc)
		require.Equal(t, 512*2, size)

		page = bytes.Repeat([]byte{1}, 512)
		require.Equal(t, 0, testPageFileWrite(f, page, 512))
		page = bytes.Repeat([]byte{2}, 512)
		require.Equal(t, 0, testPageFileWrite(f, page, 1024))

		// Expect written pages are immediately read-able.
		page = make([]byte, 512)
		require.Equal(t, 0, testPageFileRead(f, page, 0))
		require.Equal(t, makePageFileHeader(512, 2), page)
		require.Equal(t, 0, testPageFileRead(f, page, 512))
		require.Equal(t, bytes.Repeat([]byte{1}, 512), page)
		require.Equal(t, 0, testPageFileRead(f, page, 1024))
		require.Equal(t, bytes.Repeat([]byte{2}, 512), page)

		// Expect page size reflects the third page.
		size, rc = testPageFileSize(f)
		require.Equal(t, 0, rc)
		require.Equal(t, 512*3, size)

		// Expect no pages have been written yet.
		require.Empty(t, enumPages(t, store.pages, store.PageDBColumns["/page-rw.file"]))
		// Expect that, after syncing, pages are visible within the DB.
		require.Equal(t, 0, testPageFileSync(f))
		require.Equal(t, map[uint64][]byte{
			0:    makePageFileHeader(512, 2),
			512:  bytes.Repeat([]byte{1}, 512),
			1024: bytes.Repeat([]byte{2}, 512),
		}, enumPages(t, store.pages, store.PageDBColumns["/page-rw.file"]))
	})

	t.Run("pageFlushAndRollback", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "page-flush.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainDB)
		require.Equal(t, 0, rc)

		// Write header and 3 data pages. The 3rd data page over-runs the
		// target batch size and flushes to the pages DB.
		var page = makePageFileHeader(512, 1)
		require.Equal(t, 0, testPageFileWrite(f, page, 0*512))
		for i := 1; i != 4; i++ {
			page = bytes.Repeat([]byte{byte(i)}, 512)
			require.Equal(t, 0, testPageFileWrite(f, page, int64(i*512)))
		}

		// Expect file size reflects all pages.
		size, rc := testPageFileSize(f)
		require.Equal(t, 0, rc)
		require.Equal(t, 4*512, size)

		// And all but the last data page flush to the DB.
		require.Equal(t, map[uint64][]byte{
			0 * 512: makePageFileHeader(512, 1),
			1 * 512: bytes.Repeat([]byte{1}, 512),
			2 * 512: bytes.Repeat([]byte{2}, 512),
		}, enumPages(t, store.pages, store.PageDBColumns["/page-flush.file"]))

		// Simulate a rollback, by updating the header with a smaller page-count.
		page = makePageFileHeader(512, 2)
		require.Equal(t, 0, testPageFileWrite(f, page, 0))

		// Trim is immediately reflected in file size.
		size, rc = testPageFileSize(f)
		require.Equal(t, 0, rc)
		require.Equal(t, 2*512, size)

		// Sync. Expect all but the first two pages were deleted.
		require.Equal(t, 0, testPageFileSync(f))
		require.Equal(t, map[uint64][]byte{
			0 * 512: makePageFileHeader(512, 2),
			1 * 512: bytes.Repeat([]byte{1}, 512),
		}, enumPages(t, store.pages, store.PageDBColumns["/page-flush.file"]))
	})

	t.Run("pageBatchAtomicWrites", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "atomic.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainDB)
		require.Equal(t, 0, rc)

		// Atomic write of a number of pages (more than we would otherwise buffer).
		require.Equal(t, 0, testPageFileControl(f, kSqliteFcntlBeginAtomicWrite))
		var page = makePageFileHeader(512, 5)
		require.Equal(t, 0, testPageFileWrite(f, page, 0*512))

		for i := 1; i != 5; i++ {
			page = bytes.Repeat([]byte{byte(i)}, 512)
			require.Equal(t, 0, testPageFileWrite(f, page, int64(i*512)))
		}
		// Expect no pages were written yet.
		require.Empty(t, enumPages(t, store.pages, store.PageDBColumns["/atomic.file"]))

		// Commit, and expect all pages are visible.
		require.Equal(t, 0, testPageFileControl(f, kSqliteFcntlCommitAtomicWrite))
		require.Equal(t, map[uint64][]byte{
			0 * 512: makePageFileHeader(512, 5),
			1 * 512: bytes.Repeat([]byte{1}, 512),
			2 * 512: bytes.Repeat([]byte{2}, 512),
			3 * 512: bytes.Repeat([]byte{3}, 512),
			4 * 512: bytes.Repeat([]byte{4}, 512),
		}, enumPages(t, store.pages, store.PageDBColumns["/atomic.file"]))

		// Start another atomic write, and roll it back.
		require.Equal(t, 0, testPageFileControl(f, kSqliteFcntlBeginAtomicWrite))
		for i := 1; i != 3; i++ {
			page = bytes.Repeat([]byte{byte(10 * i)}, 512)
			require.Equal(t, 0, testPageFileWrite(f, page, int64(i*512)))
		}
		require.Equal(t, 0, testPageFileControl(f, kSqliteFcntlRollbackAtomicWrite))

		// Issue a regular write, and sync.
		page = bytes.Repeat([]byte{'a'}, 512)
		require.Equal(t, 0, testPageFileWrite(f, page, 3*512))
		require.Equal(t, 0, testPageFileSync(f))

		// Expect the rollback writes were dropped (and the following regular write wasn't).
		require.Equal(t, map[uint64][]byte{
			0 * 512: makePageFileHeader(512, 5),
			1 * 512: bytes.Repeat([]byte{1}, 512),
			2 * 512: bytes.Repeat([]byte{2}, 512),
			3 * 512: bytes.Repeat([]byte{'a'}, 512),
			4 * 512: bytes.Repeat([]byte{4}, 512),
		}, enumPages(t, store.pages, store.PageDBColumns["/atomic.file"]))
	})

	t.Run("pageErrors", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "errors.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainDB)
		require.Equal(t, 0, rc)

		// Non-aligned read of empty database header => IOERR_SHORT_READ.
		var page = make([]byte, 30)
		require.Equal(t, int(sqlite3.ErrIoErrShortRead),
			testPageFileRead(f, page, 10))

		page = makePageFileHeader(512, 1)
		require.Equal(t, 0, testPageFileWrite(f, page, 0))

		// Attempt to read more than a page => SQLITE_CORRUPT.
		require.Equal(t, int(sqlite3.ErrCorrupt),
			testPageFileRead(f, make([]byte, 1024), 0))

		// Attempt to change page size after it's been set => SQLITE_MISUSE.
		page = makePageFileHeader(1024, 1)
		require.Equal(t, int(sqlite3.ErrIoErrWrite),
			testPageFileWrite(f, page, 0))
	})

	t.Run("logReadAndWrite", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "log-rw.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainJournal)
		require.Equal(t, 0, rc)

		var fnode = fsm.Links["/log-rw.file"]
		var seqNo = fsm.LiveNodes[fnode].Segments[0].FirstSeqNo

		// Issue a write. Expect it's delegated: a file was created on-disk with our write.
		require.Equal(t, 0, testLogFileWrite(f, makeU32(kHotJournalHeader), 0))
		var b, err = ioutil.ReadFile(filepath.Join(tmpdir, "/log-rw.file"))
		require.NoError(t, err)
		require.Equal(t, makeU32(kHotJournalHeader), b)

		// However, the write is buffered / not delivered over CGO until the file is synced.
		require.Equal(t, seqNo, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
		require.Equal(t, 0, testLogFileSync(f))
		require.Equal(t, seqNo+1, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
	})

	t.Run("logBufferAndFlush", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "log-flush.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainJournal)
		require.Equal(t, 0, rc)

		var fnode = fsm.Links["/log-flush.file"]
		var seqNo = fsm.LiveNodes[fnode].Segments[0].FirstSeqNo

		// Fill file buffer, but don't yet flush.
		require.Equal(t, 0, testLogFileWrite(f, makeU32(kHotJournalHeader), 0))
		var b = bytes.Repeat([]byte("a"), int(fileBufferSize-4))
		require.Equal(t, 0, testLogFileWrite(f, b, 4))
		require.Equal(t, seqNo, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)

		// Expect next write causes a buffer flush.
		require.Equal(t, 0, testLogFileWrite(f, makeU32(kHotJournalHeader), int64(fileBufferSize)))
		require.Equal(t, seqNo+1, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
		seqNo++

		// A small write at a non-continuous offset also flushes the buffer.
		require.Equal(t, 0, testLogFileWrite(f, []byte("hello"), 123))
		require.Equal(t, seqNo+1, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
		seqNo++
		require.Equal(t, 0, testLogFileSync(f))
		require.Equal(t, seqNo+1, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
		seqNo++

		// A very large write results in multiple buffer-size flushes.
		b = bytes.Repeat([]byte("b"), int(2*fileBufferSize+10))
		require.Equal(t, 0, testLogFileWrite(f, b, 456))
		require.Equal(t, seqNo+2, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
		seqNo += 2
		require.Equal(t, 0, testLogFileSync(f))
		require.Equal(t, seqNo+1, fsm.LiveNodes[fnode].Segments[0].LastSeqNo)
		seqNo++
	})

	t.Run("logTruncate", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "log-trunc.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainJournal)
		require.Equal(t, 0, rc)

		// Write some initial content.
		var fnode = fsm.Links["/log-trunc.file"]
		require.Equal(t, 0, testLogFileWrite(f, makeU32(kHotJournalHeader), 0))

		// Not an error to truncate to the implied file length (though we haven't flushed).
		require.Equal(t, 0, testLogFileTruncate(f, 4))
		// However, it *is* an error to truncate to anything else without syncing.
		require.Equal(t, int(sqlite3.ErrIoErrTruncate), testLogFileTruncate(f, 0))
		require.Equal(t, int(sqlite3.ErrIoErrTruncate), testLogFileTruncate(f, 3))
		require.Equal(t, 0, testLogFileSync(f))

		// Truncate to non-zero offset is a recorder no-op.
		require.Equal(t, 0, testLogFileTruncate(f, 3))
		require.Equal(t, fnode, fsm.Links["/log-trunc.file"])

		// Truncating to zero, however, records as a delete-and-create with a new empty FNode.
		require.Equal(t, 0, testLogFileTruncate(f, 0))
		require.NotEqual(t, fnode, fsm.Links["/log-trunc.file"])

		fnode = fsm.Links["/log-trunc.file"]
		var segment = fsm.LiveNodes[fnode].Segments[0]
		require.Equal(t, segment.FirstSeqNo, segment.LastSeqNo)
	})

	t.Run("logRestart", func(t *testing.T) {
		var f, rc = testRecFSOpen(store.vfs, "log-restart.file",
			kSqliteOpenCreate|kSqliteOpenReadwrite|kSqliteOpenMainJournal)
		require.Equal(t, 0, rc)

		var fnode = fsm.Links["/log-restart.file"]
		require.Equal(t, 0, testLogFileWrite(f, makeU32(kHotJournalHeader), 0))
		require.Equal(t, 0, testLogFileSync(f))

		// On re-writes of a "hot" journal header, or an unknown header, log is _not_ restarted.
		for _, hdr := range []uint32{kHotJournalHeader, 12345678} {
			require.Equal(t, 0, testLogFileWrite(f, makeU32(hdr), 0))
			require.Equal(t, 0, testLogFileSync(f))
			require.Equal(t, fnode, fsm.Links["/log-restart.file"])
		}
		// Re-writes of zero or WAL headers _do_ restart the log.
		for _, hdr := range []uint32{0, kWALHeaderBigEndian, kWALHeaderLittleEndian} {
			require.Equal(t, 0, testLogFileWrite(f, makeU32(hdr), 0))
			require.Equal(t, 0, testLogFileSync(f))

			var next = fsm.Links["/log-restart.file"]
			require.NotEqual(t, fnode, next)
			fnode = next
		}
		// Attempting to restart *without* a prior sync is an error.
		require.Equal(t, 0, testLogFileWrite(f, []byte("hello"), 4))
		require.Equal(t, int(sqlite3.ErrIoErrWrite),
			testLogFileWrite(f, makeU32(kHotJournalHeader), 0))
	})

	for op := range ajc.PendingExcept("") {
		<-op.Done()
	}
	cleanup()
}

// runStatement executes |stmt| against |db1| & |db2|, and confirms they emit
// identical results.
func runStatement(t *testing.T, stmt string, sr1, sr2 *strings.Replacer, db1, db2 *sql.DB) {
	r1, err := db1.Query(sr1.Replace(stmt))
	require.NoError(t, err)
	r2, err := db2.Query(sr2.Replace(stmt))
	require.NoError(t, err)

	c1, err := r1.Columns()
	require.NoError(t, err)
	c2, err := r2.Columns()
	require.NoError(t, err)
	require.Equal(t, c1, c2)

	s1, s2 := make([]string, len(c1)), make([]string, len(c2))
	i1, i2 := make([]interface{}, len(c1)), make([]interface{}, len(c2))

	for i := range s1 {
		i1[i], i2[i] = &s1[i], &s2[i]
	}
	for n1, n2 := true, true; n1 && n2; {
		n1 = r1.Next()
		n2 = r2.Next()

		if n1 {
			require.NoError(t, r1.Scan(i1...))
		}
		if n2 {
			require.NoError(t, r2.Scan(i2...))
		}
		require.Equal(t, n1, n2)
		require.Equal(t, s1, s2)
	}
	require.NoError(t, r1.Close())
	require.NoError(t, r2.Close())
}

// runSequenceTest runs the sequence SQL file at |path| against a Store
// and an unmodified "golden" SQLite DB, and confirms they emit identical
// results. A special token CRASH_AND_RECOVER causes the Store instance
// to be re-created (in a new temp directory) by injecting a handoff and
// recovering from the log.
func runSequenceTest(t *testing.T, path string, uriArgs map[string]string) {
	var bk, cleanup = newBrokerAndLog(t)
	defer cleanup()

	var fin, err = os.Open(path)
	require.NoError(t, err)
	var br = bufio.NewReader(fin)

	goldDir, err := ioutil.TempDir("", "sqlite-seq-gold")
	require.NoError(t, err)

	var goldDB *sql.DB
	defer func() {
		if goldDB != nil {
			require.NoError(t, goldDB.Close())
		}
		require.NoError(t, os.RemoveAll(goldDir))
	}()

	var (
		store      *Store
		prevCancel context.CancelFunc
		hints      = recoverylog.FSMHints{Log: aRecoveryLog}
	)
	for {
		var ctx, cancel = context.WithCancel(context.Background())
		var ajc = client.NewAppendService(ctx, bk.Client())
		var player = recoverylog.NewPlayer()
		var author = recoverylog.NewRandomAuthor()
		var tmpdir, err = ioutil.TempDir("", "sqlite-seq-test")
		require.NoError(t, err)

		go func() {
			require.NoError(t, player.Play(ctx, hints, tmpdir, ajc))
		}()
		player.InjectHandoff(author)
		<-player.Done()
		require.NotNil(t, player.Resolved.FSM)

		if store != nil {
			prevCancel()
			store.Destroy()
		}
		var recorder = recoverylog.NewRecorder(
			aRecoveryLog,
			player.Resolved.FSM,
			author,
			tmpdir,
			ajc,
		)

		store, err = NewStore(recorder)
		require.NoError(t, err)

		for k, v := range uriArgs {
			store.SQLiteURIValues.Set(k, v)
		}
		require.NoError(t, store.Open(""))

		goldDB, err = sql.Open("sqlite3", fmt.Sprintf("file:%s/main.db", goldDir))
		require.NoError(t, err)
		_, err = goldDB.Exec(`PRAGMA synchronous=OFF;`)
		require.NoError(t, err)

		var goldSR = strings.NewReplacer(
			"ATTACH_DB1", filepath.Join(goldDir, "attach_one.db"),
			"ATTACH_DB2", filepath.Join(goldDir, "attach_two.db"),
		)
		var tstSR = strings.NewReplacer(
			"ATTACH_DB1", store.URIForDB("attach_one.db"),
			"ATTACH_DB2", store.URIForDB("attach_two.db"),
		)
		for {
			stmt, err := br.ReadString(';')
			if err == io.EOF {
				cancel()
				store.Destroy()
				return
			} else if strings.Count(stmt, "CRASH_AND_RECOVER") != 0 {
				break
			} else {
				runStatement(t, stmt, goldSR, tstSR, goldDB, store.SQLiteDB)
			}
		}

		prevCancel = cancel
		hints, err = recorder.BuildHints()
		require.NoError(t, err)
		require.NoError(t, goldDB.Close())
	}
}

// makePageFileHeader generates a SQLite DB file header fixture.
func makePageFileHeader(pageSize uint16, pageCount uint32) []byte {
	var page [512]byte

	binary.BigEndian.PutUint16(page[16:], pageSize)
	binary.BigEndian.PutUint32(page[28:], pageCount)
	binary.BigEndian.PutUint32(page[24:], 1111111111) // Change counter.
	binary.BigEndian.PutUint32(page[32:], 2222222222) // Freelist head.
	binary.BigEndian.PutUint32(page[36:], 3333333333) // Freelist count.
	return page[:]
}

func makeU32(v uint32) []byte {
	var b = make([]byte, 4)
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

// enumPages returns a map of all pages in a pageDB column family.
func enumPages(t *testing.T, db *gorocksdb.DB, cf *gorocksdb.ColumnFamilyHandle) map[uint64][]byte {
	var ro = gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	var it = db.NewIteratorCF(ro, cf)
	defer it.Close()

	var m = make(map[uint64][]byte)

	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		var key, value = it.Key(), it.Value()
		m[binary.BigEndian.Uint64(key.Data())] = append([]byte(nil), value.Data()...)
		key.Free()
		value.Free()
	}
	require.NoError(t, it.Err())
	return m
}

func newBrokerAndLog(t require.TestingT) (*brokertest.Broker, func()) {
	var etcd = etcdtest.TestClient()
	var broker = brokertest.NewBroker(t, etcd, "local", "broker")

	brokertest.CreateJournals(t, broker, brokertest.Journal(pb.JournalSpec{Name: aRecoveryLog}))

	return broker, func() {
		broker.Tasks.Cancel()
		require.NoError(t, broker.Tasks.Wait())
		etcdtest.Cleanup()
	}
}

const aRecoveryLog pb.Journal = "test/store-sqlite/recovery-log"

func TestMain(m *testing.M) { etcdtest.TestMainWithEtcd(m) }
