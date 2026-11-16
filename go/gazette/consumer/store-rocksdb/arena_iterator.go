package store_rocksdb

/*
#include <stdlib.h>
#include "rocksdb/c.h"

size_t arena_iter_next(rocksdb_iterator_t* cit, size_t arena_len, char* arena);

*/
import "C"
import (
	"bytes"
	"unsafe"

	"github.com/jgraettinger/gorocksdb"
)

// ArenaIterator adapts a gorocksdb.Iterator to amortize CGO calls by
// pre-fetching multiple keys and values into a memory arena. This is generally
// a performance win in the common case of stepping through via Next() with
// minimal seeking.
//
// It's interface and semantics matches that of gorocksdb.Iterator, with the
// exception that Key() and Value() return []byte rather than a gorocksdb.Slice.
type ArenaIterator struct {
	delegate gorocksdb.Iterator

	// CGO calls are expensive. We amortize this cost by batching iteration and
	// key/value retrieval into a memory arena, populated from C++ and read from
	// Go. For the common case, this greatly reduces the number of required CGO
	// calls in exchange for an extra copy.
	arena, remainder []byte
}

// AsArenaIterator adapts a gorocksdb.Iterator to an ArenaIterator that pre-fetches
// multiple keys & values into |arena| via a single CGO call.
func AsArenaIterator(it *gorocksdb.Iterator, arena []byte) *ArenaIterator {
	return &ArenaIterator{delegate: *it, arena: arena}
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (it *ArenaIterator) Valid() bool {
	if len(it.remainder) != 0 {
		return true
	}
	return it.delegate.Valid()
}

// ValidForPrefix returns false only when an Iterator has iterated past the
// first or the last key in the database or the specified prefix.
func (it *ArenaIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix(it.Key(), prefix)
}

// Key returns the key the iterator currently holds.
func (it *ArenaIterator) Key() []byte {
	if len(it.remainder) != 0 {
		return it.remainder[4 : 4+parseLenPrefix(it.remainder)]
	}
	return it.delegate.Key().Data()
}

// Value returns the value in the database the iterator currently holds.
func (it *ArenaIterator) Value() []byte {
	if len(it.remainder) != 0 {
		var keyLen = parseLenPrefix(it.remainder)
		return it.remainder[8+keyLen : 8+keyLen+parseLenPrefix(it.remainder[4+keyLen:])]
	}
	return it.delegate.Value().Data()
}

// Next moves the iterator to the next sequential key in the database.
func (it *ArenaIterator) Next() {
	if len(it.remainder) != 0 {
		// Step past current key & value.
		it.remainder = it.remainder[4+parseLenPrefix(it.remainder):]
		it.remainder = it.remainder[4+parseLenPrefix(it.remainder):]

		if len(it.remainder) != 0 {
			return // Next value already in the arena.
		}
	}

	// We cannot directly reach into delegate and reference it's *C.rocksdb_iterator_t.
	// Instead we reinterpret-cast into a private struct of the same size & layout.
	var offset = C.arena_iter_next(
		(*gorocksdbIterator)(unsafe.Pointer(&it.delegate)).c,
		C.size_t(len(it.arena)),
		byteToChar(it.arena))

	it.remainder = it.arena[:offset]
}

// Prev moves the iterator to the previous sequential key in the database.
func (it *ArenaIterator) Prev() {
	if len(it.remainder) != 0 {
		it.Seek(it.Key()) // Seek back to the current key before stepping to Prev.
	}
	it.delegate.Prev()
}

// SeekToFirst moves the iterator to the first key in the database.
func (it *ArenaIterator) SeekToFirst() {
	it.delegate.SeekToFirst()
	it.remainder = nil
}

// SeekToLast moves the iterator to the last key in the database.
func (it *ArenaIterator) SeekToLast() {
	it.delegate.SeekToLast()
	it.remainder = nil
}

// Seek moves the iterator to the position greater than or equal to the key.
func (it *ArenaIterator) Seek(key []byte) {
	it.delegate.Seek(key)
	it.remainder = nil
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (it *ArenaIterator) SeekForPrev(key []byte) {
	it.delegate.SeekForPrev(key)
	it.remainder = nil
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (it *ArenaIterator) Err() error { return it.delegate.Err() }

// Close closes the iterator.
func (it *ArenaIterator) Close() {
	it.delegate.Close()
	it.remainder = nil
}

func byteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}
	return c
}

func parseLenPrefix(b []byte) C.size_t {
	return C.size_t(b[0])<<24 | C.size_t(b[1])<<16 | C.size_t(b[2])<<8 | C.size_t(b[3])
}

type gorocksdbIterator struct {
	c *C.rocksdb_iterator_t
}

func init() {
	if unsafe.Sizeof(new(gorocksdb.Iterator)) != unsafe.Sizeof(new(gorocksdbIterator)) ||
		unsafe.Alignof(new(gorocksdb.Iterator)) != unsafe.Alignof(new(gorocksdbIterator)) {
		panic("did gorocksdb.Iterator change? store-rocksdb cannot safely reinterpret-cast")
	}
}
