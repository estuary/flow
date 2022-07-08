//go:build linux

package flow

import "syscall"

// Deliver a SIGTERM to the process if this thread should die uncleanly.
func JSWorkerSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}
