// +build !norocksdb

package recoverylog

import "github.com/tecbot/gorocksdb"

// RecordedRocksDB adapts a Recorder to be a rocksdb.EnvObserver
type RecordedRocksDB struct{ *Recorder }

type recordedRocksDBFile struct{ *FileRecorder }

func (r RecordedRocksDB) NewWritableFile(path string) gorocksdb.WritableFileObserver {
	return recordedRocksDBFile{r.RecordCreate(path)}
}

func (r RecordedRocksDB) DeleteFile(path string) { r.RecordRemove(path) }

func (r RecordedRocksDB) DeleteDir(dirname string) { panic("not supported") }

func (r RecordedRocksDB) RenameFile(src, target string) { r.RecordRename(src, target) }

func (r RecordedRocksDB) LinkFile(src, target string) { r.LinkFile(src, target) }

func (r recordedRocksDBFile) Append(data []byte) { r.RecordWrite(data) }

func (r recordedRocksDBFile) Close() {} // No-op.

func (r recordedRocksDBFile) Sync()                          { <-r.Recorder.StrongBarrier().Done() }
func (r recordedRocksDBFile) Fsync()                         { <-r.Recorder.StrongBarrier().Done() }
func (r recordedRocksDBFile) RangeSync(offset, nbytes int64) { <-r.Recorder.StrongBarrier().Done() }
