//go:build aix || darwin || (js && wasm) || (solaris && !illumos)

package main

import "syscall"

// Darwin does not support Pdeathsig so we just leave a default SysProcAttr
func SysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}
