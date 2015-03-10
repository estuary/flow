// +build darwin dragonfly freebsd linux netbsd openbsd
package gazette

import (
	"os"
	"syscall"
)

type unixLockedFile struct {
	file *os.File
}

func openLockedFile(path string, flag int, perm os.FileMode) (
	lockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = setFileLock(f, true); err != nil {
		f.Close()
		return nil, err
	}
	return &unixLockedFile{file: f}, nil
}

func (f *unixLockedFile) File() *os.File {
	return f.file
}

func (f *unixLockedFile) Close() error {
	if err := setFileLock(f.file, false); err != nil {
		return err
	}
	return f.file.Close()
}

func setFileLock(f *os.File, lock bool) error {
	how := syscall.LOCK_UN
	if lock {
		how = syscall.LOCK_EX
	}
	return syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
}
