// +build darwin

package journal

import "syscall"

func fdatasync(fd int) error {
	return syscall.Fsync(fd)
}
