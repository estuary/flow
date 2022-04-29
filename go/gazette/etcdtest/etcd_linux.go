//go:build linux
// +build linux

package etcdtest

import "syscall"

var getSysProcAttr = func() *syscall.SysProcAttr {
	// If this process dies (e.x, due to an uncaught panic from a
	// test timeout), deliver a SIGTERM to the `etcd` process).
	// This ensures a wrapping `go test` doesn't hang forever awaiting
	// the `etcd` child to exit.
	res := new(syscall.SysProcAttr)
	res.Pdeathsig = syscall.SIGTERM
	return res
}
