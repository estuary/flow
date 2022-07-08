//go:build linux

package main

import "syscall"

// Deliver a SIGTERM to the process if this thread should die uncleanly.
func SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}
