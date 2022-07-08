//go:build aix || darwin || (js && wasm) || (solaris && !illumos)

package bindings

import (
	"os"
	"syscall"
)

// We use a direct syscall here instead of `os.Pipe` because we need to ensure that _only_ the
// Rust side closes the file. `os.Pipe` returns `os.File`s, which will always close themselves
// when they are garbage collected, so we cannot create a `File` for the writer without it being
// closed by Go. The syscall returns raw file descriptors, though, which does exactly what we
// want. The syscall here was modeled after the one from `os.Pipe`.

// Pipe returns a connected pair of Files; reads from r return bytes written to w.
// It returns the files and an error, if any.
func Pipe() (r *os.File, wDescriptor int, err error) {
	var pipeFileDescriptors [2]int

	// See ../syscall/exec.go for description of lock.
	syscall.ForkLock.RLock()
	e := syscall.Pipe(pipeFileDescriptors[0:])
	if e != nil {
		syscall.ForkLock.RUnlock()
		return nil, 0, os.NewSyscallError("pipe", e)
	}
	syscall.CloseOnExec(pipeFileDescriptors[0])
	syscall.CloseOnExec(pipeFileDescriptors[1])
	syscall.ForkLock.RUnlock()

	return os.NewFile(uintptr(pipeFileDescriptors[0]), "|0"), pipeFileDescriptors[1], nil
}
