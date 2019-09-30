package recoverylog

import (
	"fmt"
	"os"

	"github.com/spf13/afero"
)

// RecordedAferoFS adapts a Recorder to wrap a afero.Fs instance.
type RecordedAferoFS struct {
	*Recorder
	afero.Fs
}

func (r RecordedAferoFS) Create(name string) (afero.File, error) {
	var file, err = r.Fs.Create(name)
	if err != nil {
		return file, err
	}

	return &recordedAferoFile{
		FileRecorder: FileRecorder{
			Recorder: r.Recorder,
			Fnode:    r.Recorder.RecordCreate(name),
		},
		File: file,
	}, nil
}

func (r RecordedAferoFS) OpenFile(name string, flags int, perm os.FileMode) (afero.File, error) {
	var file, err = r.Fs.OpenFile(name, flags, perm)
	if err != nil {
		return file, err
	}

	if (flags & os.O_RDONLY) != 0 {
		return file, err // Not recorded.
	} else if (flags & os.O_APPEND) != 0 {
		// NOTE(johnny): No current use-case. Could be supported by stating the file here.
		return nil, fmt.Errorf("O_APPEND not supported by RecordedAferoFS")
	}
	return &recordedAferoFile{
		FileRecorder: FileRecorder{
			Recorder: r.Recorder,
			Fnode:    r.Recorder.RecordOpenFile(name, flags),
		},
		File: file,
	}, nil
}

func (r RecordedAferoFS) Remove(name string) error {
	if err := r.Fs.Remove(name); err != nil {
		return err
	}
	r.Recorder.RecordRemove(name)
	return nil
}

func (r RecordedAferoFS) RemoveAll(path string) error {
	return fmt.Errorf("removeAll not supported by RecordedAferoFS")
}

func (r RecordedAferoFS) Rename(oldname, newname string) error {
	if err := r.Fs.Rename(oldname, newname); err != nil {
		return err
	}
	r.Recorder.RecordRename(oldname, newname)
	return nil
}

// recordedAferoFile adapts a FileRecorder to wrap a afero.File instance.
type recordedAferoFile struct {
	FileRecorder
	afero.File
}

func (r *recordedAferoFile) Write(p []byte) (n int, err error) {
	n, err = r.File.Write(p)
	r.FileRecorder.RecordWrite(p[:n])
	return
}

func (r *recordedAferoFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = r.File.WriteAt(p, off)
	r.FileRecorder.RecordWriteAt(p, off)
	return
}

func (r *recordedAferoFile) Seek(offset int64, whence int) (int64, error) {
	offset, err := r.File.Seek(offset, whence)
	r.FileRecorder.Offset = offset
	return offset, err
}

func (r *recordedAferoFile) Sync() error {
	if err := r.File.Sync(); err != nil {
		return err
	}
	var txn = r.Recorder.Barrier(nil)
	<-txn.Done()
	return txn.Err()
}

func (r *recordedAferoFile) Truncate(size int64) error {
	// NOTE(johnny): the size == 0 case could be implemented as RecordCreate.
	return fmt.Errorf("truncate not supported by RecordedAferoFS")
}

func (r *recordedAferoFile) WriteString(s string) (int, error) {
	return r.Write([]byte(s))
}
