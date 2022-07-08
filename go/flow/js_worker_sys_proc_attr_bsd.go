//go:build aix || darwin || (js && wasm) || (solaris && !illumos)

package flow

import "syscall"

// Darwin does not support Pdeathsig so we just leave a default SysProcAttr
func JSWorkerSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}
