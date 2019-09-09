package store_rocksdb

import (
	"go.gazette.dev/core/consumer/recoverylog"
)

// NewRecorder adapts a recoverylog.Recorder to an EnvObserver.
func NewRecorder(recorder *recoverylog.Recorder) EnvObserver {
	return recordedDB{rec: recorder}
}

type recordedDB struct{ rec *recoverylog.Recorder }
type recordedFile struct{ rec *recoverylog.FileRecorder }

func (r recordedDB) NewWritableFile(path string) WritableFileObserver {
	return recordedFile{rec: r.rec.RecordCreate(path)}
}
func (r recordedDB) DeleteFile(path string)        { r.rec.RecordRemove(path) }
func (r recordedDB) DeleteDir(dirname string)      { panic("not supported") }
func (r recordedDB) RenameFile(src, target string) { r.rec.RecordRename(src, target) }
func (r recordedDB) LinkFile(src, target string)   { r.rec.RecordLink(src, target) }

func (r recordedFile) Append(data []byte)              { r.rec.RecordWrite(data) }
func (r recordedFile) Close()                          {} // No-op.
func (r recordedFile) Sync()                           { <-r.rec.Barrier(nil).Done() }
func (r recordedFile) Fsync()                          { <-r.rec.Barrier(nil).Done() }
func (r recordedFile) RangeSync(offset, nbytes uint64) { <-r.rec.Barrier(nil).Done() }
