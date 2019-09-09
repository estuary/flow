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
	if file, err := r.Fs.Create(name); err != nil {
		return file, err
	} else {
		return recordedAferoFile{
			FileRecorder: r.Recorder.RecordCreate(name),
			File:         file,
		}, nil
	}
}

func (r RecordedAferoFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	var file, err = r.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return file, err
	}

	if (flag & os.O_RDONLY) != 0 {
		return file, err // Not recorded.
	}

	switch flag {
	// Allow any combination of flags which creates an exclusive new file
	// for writing, or truncates an existing file.
	case os.O_WRONLY | os.O_CREATE | os.O_TRUNC:
		fallthrough
	case os.O_RDWR | os.O_CREATE | os.O_TRUNC:
		fallthrough
	case os.O_WRONLY | os.O_CREATE | os.O_EXCL:
		fallthrough
	case os.O_RDWR | os.O_CREATE | os.O_EXCL:
		return recordedAferoFile{
			FileRecorder: r.Recorder.RecordCreate(name),
			File:         file,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported flag set %d", flag)
	}
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

type recordedAferoFile struct {
	*FileRecorder
	afero.File
}

func (r recordedAferoFile) Write(p []byte) (n int, err error) {
	n, err = r.File.Write(p)
	r.FileRecorder.RecordWrite(p[:n])
	return
}

func (r recordedAferoFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = r.File.WriteAt(p, off)

	var prev = r.FileRecorder.offset
	r.FileRecorder.offset = off
	r.FileRecorder.RecordWrite(p[:n])
	r.FileRecorder.offset = prev
	return
}

func (r recordedAferoFile) Seek(offset int64, whence int) (int64, error) {
	offset, err := r.File.Seek(offset, whence)
	r.FileRecorder.offset = offset
	return offset, err
}

func (r recordedAferoFile) Sync() error {
	if err := r.File.Sync(); err != nil {
		return err
	}
	var txn = r.Recorder.Barrier(nil)
	<-txn.Done()
	return txn.Err()
}

func (r recordedAferoFile) Truncate(int64) error {
	return fmt.Errorf("truncate not supported by RecordedAferoFS")
}

func (r recordedAferoFile) WriteString(s string) (ret int, err error) { return r.Write([]byte(s)) }
