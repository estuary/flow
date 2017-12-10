// +build linux

package journal

import "syscall"

func fdatasync(fd int) error {
	return syscall.Fdatasync(fd)
}
