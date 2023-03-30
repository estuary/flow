package bindings

/*
#include "../../crates/bindings/flow_bindings.h"
#include "rocksdb/c.h"
*/
import "C"

import (
	"unsafe"

	pr "github.com/estuary/flow/go/protocols/runtime"
	"github.com/jgraettinger/gorocksdb"
	"go.gazette.dev/core/consumer/recoverylog"
	store_rocksdb "go.gazette.dev/core/consumer/store-rocksdb"
)

func NewRocksDBDescriptor(recorder *recoverylog.Recorder) *pr.RocksDBDescriptor {
	var rocksEnv = store_rocksdb.NewHookedEnv(store_rocksdb.NewRecorder(recorder))

	// gorocksdb.Env has private field, so we must re-interpret to access.
	if unsafe.Sizeof(&gorocksdb.Env{}) != unsafe.Sizeof(&gorocksdbEnv{}) ||
		unsafe.Alignof(&gorocksdb.Env{}) != unsafe.Alignof(&gorocksdbEnv{}) {
		panic("did gorocksdb.Env change? cannot safely reinterpret-cast")
	}
	var innerPtr = uintptr(unsafe.Pointer(((*gorocksdbEnv)(unsafe.Pointer(rocksEnv))).c))

	return &pr.RocksDBDescriptor{
		RocksdbPath:      recorder.Dir(),
		RocksdbEnvMemptr: uint64(innerPtr),
	}
}

type gorocksdbEnv struct {
	c *C.rocksdb_env_t
}
