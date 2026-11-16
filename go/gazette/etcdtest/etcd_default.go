//go:build !linux
// +build !linux

package etcdtest

import "syscall"

var getSysProcAttr = func() *syscall.SysProcAttr {
	res := new(syscall.SysProcAttr)
	return res
}
