#ifndef VFS_H
#define VFS_H

#include <sqlite3.h>
#include <rocksdb/c.h>

#ifdef __cplusplus
extern "C" {
#endif

static const int kHeaderSize = 100; // As per sqlite file format spec.
static const int kMinPageSize = 1<<9; // 512 bytes.

// recFS is a sqlite3_vfs which records mutations of stateful SQLite DB files.
typedef struct recFS recFS;
// recFile is a sqlite3_file which records its mutations.
typedef struct recFile recFile;

// newRecFS returns a new recFS* of the given registered |name| and path |root|.
sqlite3_vfs *newRecFS(const char *name, const char *root);
// recFSFree frees a recFS instance.
void recFSFree(sqlite3_vfs*);

// sqlite3_vfs hooks follow:
int recFSOpen(sqlite3_vfs *p, const char *name, sqlite3_file *f, int flags, int *outFlags);
int recFSDelete(sqlite3_vfs *p, const char *name, int dirSync);
int recFSAccess(sqlite3_vfs *p, const char *name, int flags, int *out);
int recFSFullPathname(sqlite3_vfs *p, const char *name, int n, char *out);

// sqlite3_file hooks follow:
void initRecFile(recFile *rf, sqlite3_vfs *vfs, const char *name);
int recFileClose(sqlite3_file*);

// configureAsPageFile configures the file to use pageFile* VFS hooks.
void configureAsPageFile(sqlite3_file *f, rocksdb_t *db, rocksdb_column_family_handle_t *cf, size_t target_batch_size);
int pageFileRead(sqlite3_file *f, void *v, int n, sqlite3_int64 offset);
int pageFileWrite(sqlite3_file *f, const void *b, int n, sqlite3_int64 offset);
int pageFileTruncate(sqlite3_file *f, sqlite3_int64 size);
int pageFileSync(sqlite3_file *f, int flags);
int pageFileSize(sqlite3_file *f, sqlite3_int64 *out);
int pageFileControl(sqlite3_file *f, int op, void *pArg);
int pageFileDeviceChar(sqlite3_file *f);

int pageFileFlush(sqlite3_file *f);
int pageFileRefreshHeader(sqlite3_file *f);

// configureAsLogFile configures the file to use logFile* VFS hooks.
void configureAsLogFile(sqlite3_file *f, uintptr_t id, int64_t fnode, size_t buffer);
int logFileWrite(sqlite3_file *f, const void *b, int n, sqlite3_int64 offset);
int logFileTruncate(sqlite3_file *f, sqlite3_int64 size);
int logFileSync(sqlite3_file *f, int flags);
int logFileFlush(sqlite3_file *f, sqlite3_int64 next_offset);

// pageFileHeader is a representation of the SQLite DB header.
typedef struct pageFileHeader {
  uint32_t page_size;
  uint32_t page_count;
  uint32_t change_counter;
  uint32_t freelist_head;
  uint32_t freelist_count;
} pageFileHeader;

void parsePageFileHeader(const char *b, pageFileHeader *hdr);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif
