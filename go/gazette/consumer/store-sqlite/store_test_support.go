package store_sqlite

/*
#include "store.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)

// This file contains (very) thin wrappers around store.cpp functions to
// facilitate testing (CGO can't be called from store_test.go directly).
type sqlite3File C.sqlite3_file

func testRecFSOpen(p *C.sqlite3_vfs, name string, flags int) (*sqlite3File, int) {
	var f = (*sqlite3File)(C.malloc(C.ulong(p.szOsFile)))
	var cName = C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var rc = C.recFSOpen(p, cName, (*C.sqlite3_file)(f), C.int(flags), nil)
	return f, int(rc)
}

func testPageFileRead(f *sqlite3File, b []byte, offset int64) int {
	return int(C.pageFileRead(
		(*C.sqlite3_file)(f),
		unsafe.Pointer(&b[0]),
		C.int(len(b)),
		C.longlong(offset)))
}

func testPageFileWrite(f *sqlite3File, b []byte, offset int64) int {
	return int(C.pageFileWrite(
		(*C.sqlite3_file)(f),
		unsafe.Pointer(&b[0]),
		C.int(len(b)),
		C.longlong(offset)))
}

func testPageFileSync(f *sqlite3File) int {
	return int(C.pageFileSync((*C.sqlite3_file)(f), 0))
}

func testPageFileSize(f *sqlite3File) (size, code int) {
	var out C.longlong
	var rc = C.pageFileSize((*C.sqlite3_file)(f), &out)
	return int(out), int(rc)
}

func testPageFileControl(f *sqlite3File, op int) int {
	return int(C.pageFileControl((*C.sqlite3_file)(f), C.int(op), nil))
}

func testLogFileWrite(f *sqlite3File, b []byte, offset int64) int {
	return int(C.logFileWrite(
		(*C.sqlite3_file)(f),
		unsafe.Pointer(&b[0]),
		C.int(len(b)),
		C.longlong(offset)))
}

func testLogFileTruncate(f *sqlite3File, size int64) int {
	return int(C.logFileTruncate((*C.sqlite3_file)(f), C.longlong(size)))
}

func testLogFileSync(f *sqlite3File) int {
	return int(C.logFileSync((*C.sqlite3_file)(f), 0))
}

func testParsePageFileHeader(b []byte) (pageSize, pageCount, changeCtr, freeHead, freeCount int) {
	var hdr C.pageFileHeader
	C.parsePageFileHeader((*C.char)(unsafe.Pointer(&b[0])), &hdr)

	pageSize = int(hdr.page_size)
	pageCount = int(hdr.page_count)
	changeCtr = int(hdr.change_counter)
	freeHead = int(hdr.freelist_head)
	freeCount = int(hdr.freelist_count)
	return
}
