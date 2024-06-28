
#include "_cgo_export.h"
#include "store.h"
#include <iostream>
#include <sstream>
#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#ifdef __APPLE__

#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)

#endif

// recFS is a sqlite3_vfs which records mutations of stateful SQLite DB files.
struct recFS {
  sqlite3_vfs base;      // C-style subclass (is-a sqlite3_vfs).
  sqlite3_vfs *delegate; // VFS to which we delegate most operations.
  std::string name;      // Registered name of this VFS.
  std::string root;      // Directory which implicitly roots all named files.
};

// recFile is a sqlite3_file which records its mutations.
struct recFile {
  sqlite3_file base;          // C-style subclass (is-a sqlite3_file). Points at |methods|.
  sqlite3_io_methods methods; // Callback function pointers.
  sqlite3_vfs *vfs;           // Associated recFS of this recFile.
  std::string fullpath;       // Full path of this recFile.

  // Fields of page files:

  rocksdb::DB *pages;               // DB backing pages of this file.
  rocksdb::ColumnFamilyHandle *cf;  // Column family of this file.
  // WriteBatch of pages written but not yet synced (or, if atomic
  // commit is being used, pages not yet committed).
  std::unique_ptr<rocksdb::WriteBatchWithIndex> wb;
  bool in_atomic_batch;             // Are we inside a SQLite atomic write batch?
  size_t target_batch_size;         // Target size of non-atomic write batches.
  pageFileHeader cached_hdr;        // Cached file header.

  // Fields of transaction log files:

  int64_t fnode;                // recoverylog.Fnode of this log file.
  std::vector<uint8_t> pending; // Data not yet delivered over CGO.
  sqlite3_int64 pending_offset; // First write offset of |pending| data.

  sqlite3_file *delegate() {
    return reinterpret_cast<sqlite3_file*>(&this[1]);
  }
};

#define RFS_DELEGATE(RFS, Method, ...) (RFS->delegate->Method(RFS->delegate, ##__VA_ARGS__))
#define RF_DELEGATE(RF, Method, ...) (RF->delegate()->pMethods->Method(RF->delegate(), ##__VA_ARGS__))
#define RF_RETURN_ERR(RF, Err, Fmt, ...) \
({ \
  fprintf(stderr, "%s: error (" #Err "): " Fmt "\n", RF->fullpath.c_str(), ##__VA_ARGS__); \
  return Err; \
})
#define RF_LOG(RF) std::cerr << RF->fullpath << " "

// Struct definitions copied from the RocksDB C bindings (c.cc),
// to allow access to the wrapped C++ representations.
struct rocksdb_t                       { rocksdb::DB*                 rep; };
struct rocksdb_column_family_handle_t  { rocksdb::ColumnFamilyHandle* rep; };

// pageKey maps a page offset to its RocksDB key.
std::array<char, 8> pageKey(sqlite3_int64 offset);

sqlite3_vfs *newRecFS(const char *name, const char *root) {
  sqlite3_vfs *delegate = sqlite3_vfs_find(0);

  auto *vfs = new(recFS);
  vfs->delegate = delegate;
  vfs->name.assign(name);
  vfs->root.assign(root);

  vfs->base = sqlite3_vfs{
    .iVersion = delegate->iVersion,
    .szOsFile = delegate->szOsFile + int(sizeof(recFile)),
    .mxPathname = delegate->mxPathname - int(vfs->root.size()),
    .pNext = nullptr,
    .zName = vfs->name.c_str(),
    .pAppData = nullptr,
    .xOpen = recFSOpen,
    .xDelete = recFSDelete,
    .xAccess = recFSAccess,
    .xFullPathname = recFSFullPathname,
    .xDlOpen = [](sqlite3_vfs *p, const char *a) -> void * {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xDlOpen, a);
    },
    .xDlError = [](sqlite3_vfs *p, int a, char *b) {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xDlError, a, b);
    },
    .xDlSym = [](sqlite3_vfs *p, void *a, const char *b) -> void (*)() {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xDlSym, a, b);
    },
    .xDlClose = [](sqlite3_vfs *p, void *a) {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xDlClose, a);
    },
    .xRandomness = [](sqlite3_vfs *p, int a, char *b) -> int {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xRandomness, a, b);
    },
    .xSleep = [](sqlite3_vfs *p, int a) -> int {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xSleep, a);
    },
    .xCurrentTime = [](sqlite3_vfs *p, double *a) -> int {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xCurrentTime, a);
    },
    .xGetLastError = [](sqlite3_vfs *p, int a, char *b) -> int {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xGetLastError, a, b);
    },
    .xCurrentTimeInt64 = [](sqlite3_vfs *p, sqlite3_int64 *a) -> int {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xCurrentTimeInt64, a);
    },
    .xSetSystemCall = [](sqlite3_vfs *p, const char *a, sqlite3_syscall_ptr b) -> int {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xSetSystemCall, a, b);
    },
    .xGetSystemCall = [](sqlite3_vfs *p, const char *a) -> sqlite3_syscall_ptr {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xGetSystemCall, a);
    },
    .xNextSystemCall = [](sqlite3_vfs *p, const char *a) -> const char * {
      auto rfs = reinterpret_cast<recFS*>(p);
      return RFS_DELEGATE(rfs, xNextSystemCall, a);
    },
  };
  return reinterpret_cast<sqlite3_vfs*>(vfs);
}

void recFSFree(sqlite3_vfs *p) {
  auto rfs = reinterpret_cast<recFS*>(p);
  rfs->~recFS();
  free(p);
}

int recFSOpen(sqlite3_vfs *p, const char *name, sqlite3_file *f, int flags, int *outFlags) {
  auto rfs = reinterpret_cast<recFS*>(p);

  if (name == nullptr) {
    // This is a temporary which we immediately hand-off to the delegate.
    return RFS_DELEGATE(rfs, xOpen, nullptr, f, flags, outFlags);
  }

  auto *rf = reinterpret_cast<recFile*>(f);
  // Zero before passing to CGO, to not confuse the Go garbage collector.
  memset(rf, 0, rfs->base.szOsFile);

  // Construct the full path of this file. We must be careful to hold constant
  // the c_str() we're about to hand to the delegate, so recFile owns the
  // instance and we ensure it's live for as long as the delegate is.
  rf->fullpath.append(rfs->root);
  rf->fullpath.append(name);

  // Initialize the delegate and wrapping recFile. It inits into a pure
  // pass-through mode, and must then be explicitly configured from Go.
  int rc = RFS_DELEGATE(rfs, xOpen, rf->fullpath.c_str(), rf->delegate(), flags, outFlags);
  if (rc != SQLITE_OK) {
    return rc;
  }
  initRecFile(rf, p, name);

  auto logOpenTypes = \
    SQLITE_OPEN_MAIN_JOURNAL |   // Normal rollback journal.
    SQLITE_OPEN_MASTER_JOURNAL | // Master journal of multi-DB transaction.
    SQLITE_OPEN_SUBJOURNAL |     // Subordinate journal of multi-DB transaction.
    SQLITE_OPEN_WAL;             // Normal journal write-ahead log.

  // Call back to a Go hook to further initialize behavior for this file.
  if ((flags & SQLITE_OPEN_MAIN_DB) != 0) {
    return cgoOpenPageFile(p, const_cast<char*>(rf->fullpath.c_str()), f);
  } else if ((flags & logOpenTypes) != 0 && (flags & SQLITE_OPEN_READWRITE) != 0) {
    return cgoOpenLogFile(p, const_cast<char*>(rf->fullpath.c_str()), f, flags);
  } else {
    // All other file opens are temporary files which are not part of the
    // tracked database state. Leave the recFile in pass-through mode.
    return SQLITE_OK;
  }
}

int recFSDelete(sqlite3_vfs *p, const char *name, int dirSync) {
  auto rfs = reinterpret_cast<recFS*>(p);
  auto fullpath = std::string(rfs->root).append(name);

  int rc = RFS_DELEGATE(rfs, xDelete, fullpath.c_str(), dirSync);
  if (rc != SQLITE_OK) {
    return rc;
  }
  return cgoDelete(p, const_cast<char*>(fullpath.c_str()));
}

int recFSAccess(sqlite3_vfs *p, const char *name, int flags, int *out) {
  auto rfs = reinterpret_cast<recFS*>(p);
  auto fullpath = std::string(rfs->root).append(name);

  return RFS_DELEGATE(rfs, xAccess, fullpath.c_str(), flags, out);
}

int recFSFullPathname(sqlite3_vfs *p, const char *name, int n, char *out) {
  // Treat all |name|s as explicit paths. We explicitly root them with
  // the recFS root when calling into the delegate or CGO.
  //
  // Pretending that |name| is already rooted:
  // * Allows relative paths to be used within SQLite (eg, in ATTACH statements)
  // * Makes master / subordinate rollback journals work correctly, as they then
  //   store relative paths which are invariant to the temporary directory
  //   they're currently running in (rather than explicit paths which will break).
  assert(strlen(name) <= n);
  strcpy(out, name);
  return SQLITE_OK;
}

void initRecFile(recFile *rf, sqlite3_vfs *vfs, const char* name) {
  rf->base.pMethods = &rf->methods;
  rf->methods = sqlite3_io_methods{
    .iVersion = rf->delegate()->pMethods->iVersion,
    .xClose = recFileClose,
    .xRead = [](sqlite3_file *f, void *a, int b, sqlite3_int64 c) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xRead, a, b, c);
    },
    .xWrite = [](sqlite3_file *f, const void *a, int b, sqlite3_int64 c) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xWrite, a, b, c);
    },
    .xTruncate = [](sqlite3_file *f, sqlite3_int64 a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xTruncate, a);
    },
    .xSync = [](sqlite3_file *f, int a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xSync, a);
    },
    .xFileSize = [](sqlite3_file *f, sqlite3_int64 *a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xFileSize, a);
    },
    .xLock = [](sqlite3_file *f, int a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xLock, a);
    },
    .xUnlock = [](sqlite3_file *f, int a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xUnlock, a);
    },
    .xCheckReservedLock = [](sqlite3_file *f, int *a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xCheckReservedLock, a);
    },
    .xFileControl = [](sqlite3_file *f, int a, void *b) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xFileControl, a, b);
    },
    .xSectorSize = [](sqlite3_file *f) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xSectorSize);
    },
    .xDeviceCharacteristics = [](sqlite3_file *f) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xDeviceCharacteristics);
    },
    .xShmMap = [](sqlite3_file *f, int a, int b, int c, void volatile **d) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xShmMap, a, b, c, d);
    },
    .xShmLock = [](sqlite3_file *f, int a, int b, int c) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xShmLock, a, b, c);
    },
    .xShmBarrier = [](sqlite3_file *f) {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xShmBarrier);
    },
    .xShmUnmap = [](sqlite3_file *f, int a) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xShmUnmap, a);
    },
    .xFetch = [](sqlite3_file *f, sqlite3_int64 a, int b, void **c) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xFetch, a, b, c);
    },
    .xUnfetch = [](sqlite3_file *f, sqlite3_int64 a, void *b) -> int {
      auto rf = reinterpret_cast<recFile *>(f);
      return RF_DELEGATE(rf, xUnfetch, a, b);
    },
  };
  rf->vfs = vfs;

  // Note fullpath is already initialized.
}

int recFileClose(sqlite3_file *f) {
  auto rf = reinterpret_cast<recFile *>(f);

  if (rf->wb) {
    pageFileFlush(f);
  } else {
    logFileFlush(f, 0);
  }

  int rc = RF_DELEGATE(rf, xClose);
  rf->~recFile();
  return rc;
}

void configureAsPageFile(sqlite3_file *f, rocksdb_t *db, rocksdb_column_family_handle_t *cf, size_t target_batch_size) {
  recFile *rf = reinterpret_cast<recFile*>(f);

  rf->pages = db->rep;
  rf->cf = cf->rep;
  rf->wb.reset(new rocksdb::WriteBatchWithIndex(
      rocksdb::BytewiseComparator(), // backup_index_comparator.
      0, // reserved_bytes
      true, // overwrite_key
      0 // max_bytes
  ));
  rf->in_atomic_batch = false;
  rf->target_batch_size = target_batch_size;
  rf->cached_hdr = pageFileHeader{};

  // Swap out delegating file operations for pageFile*() versions
  // which use the given page database and column family.
  rf->methods.xRead = pageFileRead;
  rf->methods.xWrite = pageFileWrite;
  rf->methods.xTruncate = pageFileTruncate;
  rf->methods.xSync = pageFileSync;
  rf->methods.xFileSize = pageFileSize;
  rf->methods.xFileControl = pageFileControl;
  rf->methods.xDeviceCharacteristics = pageFileDeviceChar;

  // Version 2 signals xFetch / xUnfetch is not supported.
  rf->methods.iVersion = std::min(rf->methods.iVersion, 2);
  rf->methods.xFetch = nullptr;
  rf->methods.xUnfetch = nullptr;
}

int pageFileRead(sqlite3_file *f, void *b, int n, sqlite3_int64 offset) {
  // RF_LOG(rf) << " pageFileRead " << offset <<  " " << n << std::endl;
  auto rf = reinterpret_cast<recFile *>(f);
  // Reads within the header (only) may not be page-aligned.
  auto key = (offset > kHeaderSize) ? pageKey(offset) : pageKey(0);

  rocksdb::PinnableSlice page;
  auto s = rf->wb->GetFromBatchAndDB(
      rf->pages,
      rocksdb::ReadOptions(),
      rf->cf,
      rocksdb::Slice(key.data(), key.size()),
      &page);

  if (s.IsNotFound() && offset < kHeaderSize) {
    // This is an optimistic header read of an empty database. It's also the only
    // case where we expect to encounter a not-found error. In all other cases,
    // SQLite should only be reading pages known to actually exist.
    //
    // SQLITE_IOERR_SHORT_READ is required to zero un-filled portions of |b|.
    memset(b, 0, n);
    return SQLITE_IOERR_SHORT_READ;
  } else if (!s.ok()) {
    RF_RETURN_ERR(rf, SQLITE_CORRUPT, "loading page %lld: %s", offset, s.ToString().c_str());
  } else if (page.size() < kMinPageSize) {
    RF_RETURN_ERR(rf, SQLITE_CORRUPT, "page %lld is not page-length: %lu", offset, page.size());
  } else if (page.size() < n) {
    RF_RETURN_ERR(rf, SQLITE_CORRUPT, "page %lld is too short: %lu < %d", offset, page.size(), n);
  }

  memcpy(b, page.data() + (offset < kHeaderSize ? offset : 0), n);
  return SQLITE_OK;
}

int pageFileWrite(sqlite3_file *f, const void *b, int n, sqlite3_int64 offset) {
  // RF_LOG(rf) << " pageFileWrite offset " << offset <<  " amt " << n << std::endl;
  auto rf = reinterpret_cast<recFile *>(f);

  int rc = pageFileRefreshHeader(f);
  if (rc != SQLITE_OK) {
    return rc;
  }

  // If we're in a non-atomic context, perform periodic flushes of the built-up
  // write batch to the RocksDB WAL. Flushing /before/ Put avoids invalidating
  // our cached header (since the write batch is never empty on return).
  if (!rf->in_atomic_batch && (n + rf->wb->GetDataSize()) > rf->target_batch_size) {
    rc = pageFileFlush(f);
    if (rc != SQLITE_OK) {
      return rc;
    }
  }

  auto keyBytes = pageKey(offset);
  auto key = rocksdb::Slice(keyBytes.data(), keyBytes.size());
  auto page = rocksdb::Slice(reinterpret_cast<const char*>(b), n);

  // Does this write update the database header?
  if (offset == 0) {
    pageFileHeader next = {};
    parsePageFileHeader(page.data(), &next);

    /*
    RF_LOG(rf) << " header update";
    if (rf->cached_hdr.page_size != next.page_size) {
      std::cerr << " page_size " << rf->cached_hdr.page_size << "->" << next.page_size;
    }
    if (rf->cached_hdr.page_count != next.page_count) {
      std::cerr << " page_count " << rf->cached_hdr.page_count << "->" << next.page_count;
    }
    if (rf->cached_hdr.change_counter != next.change_counter) {
      std::cerr << " change_ctr " << rf->cached_hdr.change_counter << "->" << next.change_counter;
    }
    if (rf->cached_hdr.freelist_head != next.freelist_head) {
      std::cerr << " free_head " << rf->cached_hdr.freelist_head << "->" << next.freelist_head;
    }
    if (rf->cached_hdr.freelist_count != next.freelist_count) {
      std::cerr << " free_cnt " << rf->cached_hdr.freelist_count << "->" << next.freelist_count;
    }
    std::cerr << std::endl;
    */

    if (rf->cached_hdr.page_size != 0 && rf->cached_hdr.page_size != next.page_size) {
      RF_RETURN_ERR(rf, SQLITE_IOERR_WRITE,
          "attempt to change page size %u => %u", rf->cached_hdr.page_size, next.page_size);
    }

    // Trim pages which (per the header) are being dropped. We do this here, instead
    // of pageFileTruncate (a no-op), as it allows us to uniformly handle pages which
    // are already written vs still buffered in the write batch.
    while (rf->cached_hdr.page_count > next.page_count) {
      sqlite3_int64 trim = (--rf->cached_hdr.page_count) * rf->cached_hdr.page_size;
      auto trimKey = pageKey(trim);

      auto s = rf->wb->Delete( rf->cf, rocksdb::Slice(trimKey.data(), trimKey.size()));
      if (!s.ok()) {
        RF_RETURN_ERR(rf, SQLITE_IOERR, "trimming page: %s", s.ToString().c_str());
      }
    }

    rf->cached_hdr = next;
  } else {
    // All writes must be of page size and on page-aligned boundaries.
    assert(n == rf->cached_hdr.page_size);
    assert(offset % rf->cached_hdr.page_size == 0);

    // If this write extends the DB size, update our cached page count so that
    // pageFileSize() returns the correct answer in the middle of a transaction
    // (eg, after pageFileWrite and before pageFileSync is called).
    //
    // Notably, this allows SQLite to figure out that it needs to truncate the
    // database as part of a possible future ROLLBACK.
    uint32_t page_no = 1+(offset / rf->cached_hdr.page_size);
    if (page_no > rf->cached_hdr.page_count) {
      rf->cached_hdr.page_count = page_no;
    }
  }

  auto s = rf->wb->Put( rf->cf, key, page);
  if (!s.ok()) {
    RF_RETURN_ERR(rf, SQLITE_IOERR, "storing page %lld: %s", offset, s.ToString().c_str());
  }
  return SQLITE_OK;
}

int pageFileTruncate(sqlite3_file *f, sqlite3_int64 size) {
  /*
  auto rf = reinterpret_cast<recFile *>(f);

  int rc = pageFileRefreshHeader(f);
  if (rc != SQLITE_OK) {
    return rc;
  }
  RF_LOG(rf) << " pageFileTruncate " << size << " vs size hdr " \
    << (rf->cached_hdr.page_size * rf->cached_hdr.page_count) << std::endl;
  */
  return SQLITE_OK;
}

int pageFileSync(sqlite3_file *f, int flags) {
  // auto rf = reinterpret_cast<recFile *>(f);
  // RF_LOG(rf) << " pageFileSync" << std::endl;
  return pageFileFlush(f);
}

int pageFileSize(sqlite3_file *f, sqlite3_int64 *out) {
  auto rf = reinterpret_cast<recFile *>(f);

  int rc = pageFileRefreshHeader(f);
  if (rc != SQLITE_OK) {
    return rc;
  }
  *out = rf->cached_hdr.page_size * rf->cached_hdr.page_count;

  // RF_LOG(rf) << " pageFileSize " << *out << std::endl;
  return SQLITE_OK;
}

int pageFileControl(sqlite3_file *f, int op, void *pArg) {
  // RF_LOG(rf) << "pageFileControl " << op << std::endl;
  auto rf = reinterpret_cast<recFile *>(f);

  if (op == SQLITE_FCNTL_BEGIN_ATOMIC_WRITE) {
    assert(rf->wb->GetWriteBatch()->Count() == 0);
    assert(!rf->in_atomic_batch);
    rf->in_atomic_batch = true;
    return SQLITE_OK;

  } else if (op == SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE) {
    assert(rf->in_atomic_batch);
    rf->in_atomic_batch = false;
    rf->wb->Clear();
    return SQLITE_OK;

  } else if (op == SQLITE_FCNTL_COMMIT_ATOMIC_WRITE) {
    assert(rf->in_atomic_batch);
    rf->in_atomic_batch = false;
    return pageFileFlush(f);

  } else {
    return RF_DELEGATE(rf, xFileControl, op, pArg);
  }
}

int pageFileDeviceChar(sqlite3_file *f) {
  auto rf = reinterpret_cast<recFile *>(f);
  int result = RF_DELEGATE(rf, xDeviceCharacteristics);

  // We promise to never first extend a file, then *maybe* write bytes.
  // This allows SQLite to not perform an extra write of a rollback journal's
  // header, followed by a sync, before it begins writing database pages.
  result |= SQLITE_IOCAP_SAFE_APPEND;

  // Writes are applied in the order they were delivered to xWrite.
  // This allows SQLite to omit an extra sync when re-writing the WAL header.
  // However, we *do not* enable SQLITE_IOCAP_SEQUENTIAL as this causes SQLite
  // to not sync rollback journals *at all*, and we rely on this sync signal.
  // Perhaps the meaning is that it's sequential _across_ file descriptors?.
  // result |= SQLITE_IOCAP_SEQUENTIAL;

  // Upon recovery, bytes not specifically written by the application will not change.
  result |= SQLITE_IOCAP_POWERSAFE_OVERWRITE;

  // We support multiple writes which are applied as an atomic batch using:
  // - SQLITE_FCNTL_BEGIN_ATOMIC_WRITE, then
  // - SQLITE_FCNTL_COMMIT_ATOMIC_WRITE or
  // - SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE or
  // This is implemented as a RocksDB write batch.
  // Compile with -DSQLITE_ENABLE_BATCH_ATOMIC_WRITE to enable.
  result |= SQLITE_IOCAP_BATCH_ATOMIC;

  // RF_LOG(rf) << " pageDeviceChar " << result << std::endl;
  return result;
}

int pageFileFlush(sqlite3_file *f) {
  auto rf = reinterpret_cast<recFile *>(f);
  assert(!rf->in_atomic_batch);

  if (rf->wb->GetWriteBatch()->Count() == 0) {
    return SQLITE_OK; // This flush is a no-op.
  }

  auto s = rf->pages->Write(rocksdb::WriteOptions(), rf->wb->GetWriteBatch());
  if (!s.ok()) {
    RF_RETURN_ERR(rf, SQLITE_IOERR_FSYNC, "flushing write batch: %s", s.ToString().c_str());
  }
  rf->wb->Clear();
  return SQLITE_OK;
}

int pageFileRefreshHeader(sqlite3_file *f) {
  auto rf = reinterpret_cast<recFile *>(f);

  // If the write-batch is non-empty, then at least one write has occurred since
  // the last file sync or close. The implication is that an exclusive lock over
  // the paged file must be held by SQLite, and since pageFileWrite must have
  // already refreshed and updated the cached header, we can presume it's still
  // valid.
  if (rf->wb->GetWriteBatch()->Count() != 0) {
    return SQLITE_OK;
  }

  auto key = pageKey(0);
  rocksdb::PinnableSlice page;

  auto s = rf->pages->Get(
      rocksdb::ReadOptions(),
      rf->cf,
      rocksdb::Slice(key.data(), key.size()),
      &page);

  if (s.IsNotFound()) {
    rf->cached_hdr = {}; // Zero.
  } else if (!s.ok()) {
    RF_RETURN_ERR(rf, SQLITE_CORRUPT, "loading header: %s", s.ToString().c_str());
  } else if (page.size() < kMinPageSize) {
    RF_RETURN_ERR(rf, SQLITE_CORRUPT, "header is not page-length: %lu", page.size());
  } else {
    parsePageFileHeader(page.data(), &rf->cached_hdr);
  }
  return SQLITE_OK;
}

void configureAsLogFile(sqlite3_file *f, uintptr_t id, int64_t fnode, size_t buffer) {
  // RF_LOG(rf) << " configureAsLogFile" << std::endl;
  auto rf = reinterpret_cast<recFile *>(f);

  rf->fnode = fnode;
  rf->pending.reserve(buffer);

  // Swap out delegating file operations for logFile*() versions.
  rf->methods.xWrite = logFileWrite;
  rf->methods.xTruncate = logFileTruncate;
  rf->methods.xSync = logFileSync;
}

int logFileWrite(sqlite3_file *f, const void *b, int n, sqlite3_int64 offset) {
  // RF_LOG(rf) << " logFileWrite " << offset << " amt " << n << std::endl;
  auto rf = reinterpret_cast<recFile *>(f);
  auto data = reinterpret_cast<const uint8_t*>(b);

  // Require that the log is not restarted while a write is still pending.
  if (!rf->pending.empty() && offset == 0) {
    RF_RETURN_ERR(rf, SQLITE_IOERR_WRITE,
        "logFileWrite called with offset %lld, but having a pending buffer"
        " (offset %lld, size %lu) which this write does not continue (was an"
        " interleaving logFileSync omitted? ensure PRAGMA synchronous = FULL)",
        offset, rf->pending_offset, rf->pending.size());
  }

  // Dispatch to actual OS file delegate.
  int rc = RF_DELEGATE(rf, xWrite, b, n, offset);

  // Buffer writes to amortize deliveries across the (expensive) CGO barrier.
  while(rc == SQLITE_OK && n != 0) {
    if (rf->pending_offset + rf->pending.size() == offset) {
      int d = std::min<int>(n, rf->pending.capacity() - rf->pending.size());
      rf->pending.insert(rf->pending.end(), data, data + d);

      n -= d;
      data += d;
      offset += d;
    }

    if (n != 0) {
      rc = logFileFlush(f, offset);
    }
  }
  return rc;
}

int logFileTruncate(sqlite3_file *f, sqlite3_int64 size) {
  // RF_LOG(rf) << " logFileTruncate " << size << std::endl;
  auto rf = reinterpret_cast<recFile *>(f);

  if (!rf->pending.empty() && (rf->pending_offset + rf->pending.size()) != size) {
    RF_RETURN_ERR(rf, SQLITE_IOERR_TRUNCATE,
        "logFileTruncate to %lld, but having a pending buffer (offset %lld, size %lu);"
        " was an interleaving logFileSync omitted? ensure PRAGMA synchronous = FULL",
        size, rf->pending_offset, rf->pending.size());
  }

  int rc = RF_DELEGATE(rf, xTruncate, size);
  if (rc != SQLITE_OK) {
    return rc;
  }
  return cgoLogFileTruncate(
      reinterpret_cast<sqlite3_vfs*>(rf->vfs),
      &rf->fnode,
      const_cast<char*>(rf->fullpath.c_str()),
      size);
}

int logFileSync(sqlite3_file *f, int flags) {
  // RF_LOG(rf) << " logFileSync" << std::endl;
  // auto rf = reinterpret_cast<recFile *>(f);

  // Flush written content, but *don't* delegate the sync to the OS file.
  return logFileFlush(f, 0);
}

int logFileFlush(sqlite3_file *f, sqlite3_int64 next_offset) {
  auto rf = reinterpret_cast<recFile *>(f);

  int rc;
  if (!rf->pending.empty()) {
    rc = cgoLogFileWrite(
        reinterpret_cast<sqlite3_vfs*>(rf->vfs),
        &rf->fnode,
        const_cast<char *>(rf->fullpath.c_str()),
        rf->pending.data(),
        rf->pending.size(),
        rf->pending_offset);
  } else {
    rc = SQLITE_OK;
  }

  rf->pending.clear();
  rf->pending_offset = next_offset;
  return rc;
}


std::array<char, 8> pageKey(sqlite3_int64 offset) {
  // Assert |offset| is aligned to a sqlite page.
  assert(offset % kMinPageSize == 0);
  union {
    uint64_t value;
    std::array<char, 8> arr;
  } u = {.value = htobe64(uint64_t(offset))};

  return u.arr;
}

void parsePageFileHeader(const char *b, pageFileHeader *hdr) {
  hdr->page_size = be16toh(*reinterpret_cast<const int16_t*>(b+16));
  if (hdr->page_size == 1) {
    hdr->page_size = 1<<16; // 65K (special-cased by SQLite).
  }
  hdr->change_counter = be32toh(*reinterpret_cast<const uint32_t*>(b+24));
  hdr->page_count     = be32toh(*reinterpret_cast<const uint32_t*>(b+28));
  hdr->freelist_head  = be32toh(*reinterpret_cast<const uint32_t*>(b+32));
  hdr->freelist_count = be32toh(*reinterpret_cast<const uint32_t*>(b+36));
}
