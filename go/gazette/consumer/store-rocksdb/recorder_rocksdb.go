package store_rocksdb

import (
	"go.gazette.dev/core/consumer/recoverylog"
)

// NewRecorder adapts a recoverylog.Recorder to an EnvObserver.
func NewRecorder(recorder *recoverylog.Recorder) EnvObserver {
	return recordedDB{Recorder: recorder}
}

type recordedDB struct{ *recoverylog.Recorder }
type recordedFile struct{ recoverylog.FileRecorder }

func (r recordedDB) NewWritableFile(path string) WritableFileObserver {
	return &recordedFile{
		FileRecorder: recoverylog.FileRecorder{
			Recorder: r.Recorder,
			Fnode:    r.Recorder.RecordCreate(path),
		},
	}
}
func (r recordedDB) DeleteFile(path string)        { r.RecordRemove(path) }
func (r recordedDB) DeleteDir(dirname string)      { panic("not supported") }
func (r recordedDB) RenameFile(src, target string) { r.RecordRename(src, target) }
func (r recordedDB) LinkFile(src, target string)   { r.RecordLink(src, target) }

func (r *recordedFile) Append(data []byte)              { r.RecordWrite(data) }
func (r *recordedFile) Close()                          {} // No-op.
func (r *recordedFile) Sync()                           { <-r.Recorder.Barrier(nil).Done() }
func (r *recordedFile) Fsync()                          { <-r.Recorder.Barrier(nil).Done() }
func (r *recordedFile) RangeSync(offset, nbytes uint64) { <-r.Recorder.Barrier(nil).Done() }
