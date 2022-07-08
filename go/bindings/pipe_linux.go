//go:build linux

package bindings

import (
	"fmt"
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
	err = syscall.Pipe2(pipeFileDescriptors[0:], syscall.O_CLOEXEC)
	if err != nil {
		return nil, 0, fmt.Errorf("creating loging pipe: %w", err)
	}
	return os.NewFile(uintptr(pipeFileDescriptors[0]), "|0"), pipeFileDescriptors[1], nil
}
