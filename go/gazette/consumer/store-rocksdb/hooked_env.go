package store_rocksdb

/*
#include <sys/types.h>
#include "rocksdb/c.h"

rocksdb_env_t* new_hooked_env(int handle);
*/
import "C"

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/jgraettinger/gorocksdb"
)

// EnvObserver allows for observation of mutating Env operations.
// Consult |Env| in rocksdb/env.h for further details.
type EnvObserver interface {
	// Invoked just before a new WritableFile is created. Returns a
	// WritableFileObserver which is associated with the result file.
	NewWritableFile(fname string) WritableFileObserver
	// Invoked just before |fname| is deleted.
	DeleteFile(fname string)
	// Invoked just before |dirname| is deleted.
	DeleteDir(dirname string)
	// Invoked just before |src| is renamed to |target|.
	RenameFile(src, target string)
	// Invoked just before |src| is linked to |target|.
	LinkFile(src, target string)
}

// WritableFileObserver allows for observation of mutating WritableFile
// operations. Consult |WritableFile| in rocksdb/env.h for further details.
type WritableFileObserver interface {
	// Invoked when |data| is appended to the file. Note that |data| is owned by
	// RocksDB and must not be referenced after this call.
	Append(data []byte)
	// Invoked when the file is closed.
	Close()
	// Invoked when the file is Synced.
	Sync()
	// Invoked when the file is Fsync'd. Note that this may in turn
	// delegate to sync, and result in a call to the Sync() observer.
	Fsync()
	// Invoked when the file is RangeSync'd.
	RangeSync(offset, nbytes uint64)
}

// NewHookedEnv returns a "hooked" RocksDB Environment which delegates to a default
// RocksDB Environment and then informs the provided EnvObserver of method calls on
// that Environment.
func NewHookedEnv(obv EnvObserver) *gorocksdb.Env {
	// We cannot use gorocksdb.NewNativeEnv, because the CGO type *C.rocksdb_env_t
	// is private to the package, and an attempt to call NewNativeEnv fails to
	// compile with a type mismatch error. Instead we use a struct which exactly
	// matches gorocksdb.Env and the `unsafe` package to do a reinterpretation cast.
	var env = new(gorocksdb.Env)
	(*gorocksdbEnv)(unsafe.Pointer(env)).c = C.new_hooked_env(retainObserver(obv))
	return env
}

type gorocksdbEnv struct {
	c *C.rocksdb_env_t
}

//export observe_new_writable_file
func observe_new_writable_file(idx C.int, fnameRaw *C.char, fnameLen C.int) C.int {
	wf := getEnvObserver(idx).NewWritableFile(C.GoStringN(fnameRaw, fnameLen))
	return retainObserver(wf)
}

//export observe_delete_file
func observe_delete_file(idx C.int, fnameRaw *C.char, fnameLen C.int) {
	getEnvObserver(idx).DeleteFile(C.GoStringN(fnameRaw, fnameLen))
}

//export observe_delete_dir
func observe_delete_dir(idx C.int, fnameRaw *C.char, fnameLen C.int) {
	getEnvObserver(idx).DeleteDir(C.GoStringN(fnameRaw, fnameLen))
}

//export observe_rename_file
func observe_rename_file(idx C.int, srcRaw *C.char, srcLen C.int, targetRaw *C.char, targetLen C.int) {
	getEnvObserver(idx).RenameFile(C.GoStringN(srcRaw, srcLen), C.GoStringN(targetRaw, targetLen))
}

//export observe_link_file
func observe_link_file(idx C.int, srcRaw *C.char, srcLen C.int, targetRaw *C.char, targetLen C.int) {
	getEnvObserver(idx).LinkFile(C.GoStringN(srcRaw, srcLen), C.GoStringN(targetRaw, targetLen))
}

//export observe_env_dtor
func observe_env_dtor(idx C.int) {
	releaseObserver(idx)
}

//export observe_wf_dtor
func observe_wf_dtor(idx C.int) {
	releaseObserver(idx)
}

//export observe_wf_append
func observe_wf_append(idx C.int, raw *C.char, rawLen C.size_t) {
	var data []byte

	// Initialize |data| to the underlying |raw| array, without a copy.
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = uintptr(unsafe.Pointer(raw))
	header.Len = int(rawLen)
	header.Cap = int(rawLen)

	getWFObserver(idx).Append(data)
}

//export observe_wf_close
func observe_wf_close(idx C.int) {
	getWFObserver(idx).Close()
}

//export observe_wf_sync
func observe_wf_sync(idx C.int) {
	getWFObserver(idx).Sync()
}

//export observe_wf_fsync
func observe_wf_fsync(idx C.int) {
	getWFObserver(idx).Fsync()
}

//export observe_wf_range_sync
func observe_wf_range_sync(idx C.int, offset C.uint64_t, nbytes C.uint64_t) {
	getWFObserver(idx).RangeSync(uint64(offset), uint64(nbytes))
}

// liveObservers is a registry of observers which have been passed to C++
// and must be retained to guard against GC, until their matching C++
// destructor is invoked.
var liveObservers struct {
	s  []interface{}
	mu sync.Mutex
}

func retainObserver(obv interface{}) (idx C.int) {
	liveObservers.mu.Lock()
	defer liveObservers.mu.Unlock()

	for idx = 0; int(idx) != len(liveObservers.s); idx++ {
		if liveObservers.s[idx] == nil {
			liveObservers.s[idx] = obv
			return
		}
	}

	idx = C.int(len(liveObservers.s))
	liveObservers.s = append(liveObservers.s, obv)
	return
}

func releaseObserver(idx C.int) {
	liveObservers.mu.Lock()
	liveObservers.s[idx] = nil
	liveObservers.mu.Unlock()
}

func getEnvObserver(idx C.int) (r EnvObserver) {
	liveObservers.mu.Lock()
	r = liveObservers.s[idx].(EnvObserver)
	liveObservers.mu.Unlock()
	return
}

func getWFObserver(idx C.int) (wf WritableFileObserver) {
	liveObservers.mu.Lock()
	wf = liveObservers.s[idx].(WritableFileObserver)
	liveObservers.mu.Unlock()
	return
}

func init() {
	if unsafe.Sizeof(new(gorocksdb.Env)) != unsafe.Sizeof(new(gorocksdbEnv)) ||
		unsafe.Alignof(new(gorocksdb.Env)) != unsafe.Alignof(new(gorocksdbEnv)) {
		panic("did gorocksdb.Env change? store-rocksdb cannot safely reinterpret-cast")
	}
}
