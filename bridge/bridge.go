package bridge

// #cgo LDFLAGS: -lestuary_bridge -lm -ldl -L ../target/debug/
/*
#include "bindings.h"
*/
import "C"
import (
	"bufio"
	"errors"
	"reflect"
	"unsafe"

	"go.gazette.dev/core/message"
)

// Status is a returned result
func statusError(s C.status_t) error {
	var buf [64]byte
	var l = C.status_description(s, (*C.uint8_t)(&buf[0]), (C.uintptr_t)(len(buf)))
	return errors.New(string(buf[:l]))
}

// Builder wraps the native builder_t.
type Builder struct {
	ptr *C.builder_t
}

// NewBuilder constructs a new message Builder.
func NewBuilder(uuidPtr string) (Builder, error) {
	var uuidPtrC = C.CString(uuidPtr)
	defer C.free(unsafe.Pointer(uuidPtrC))

	var out *C.builder_t
	if s := C.msg_builder_new(uuidPtrC, &out); s != C.OK {
		return Builder{}, statusError(s)
	}
	return Builder{ptr: out}, nil
}

// Build a new Message instance.
func (b Builder) Build() Message {
	return Message{ptr: C.msg_builder_build(b.ptr)}
}

// Drop the Builder, releasing underlying resources.
func (b Builder) Drop() {
	C.msg_builder_drop(b.ptr)
}

// Message wraps the native message_t.
type Message struct {
	ptr *C.message_t
}

// GetUUID returns the UUID of the Message.
func (m Message) GetUUID() message.UUID {
	var uuidC = C.msg_get_uuid(m.ptr)
	// Note RFC 4122 defines that UUIDs are always network byte order (big endian).
	return message.UUID(*(*[16]byte)((unsafe.Pointer)(&uuidC.bytes[0])))
}

// SetUUID updates the UUID of the Message.
func (m Message) SetUUID(uuid message.UUID) {
	var uuidC C.uuid_t
	*(*[16]byte)((unsafe.Pointer)(&uuidC.bytes[0])) = uuid
	C.msg_set_uuid(m.ptr, uuidC)
}

func (m Message) MarshalJSONTo(b *bufio.Writer) (int, error) {
	var bufC = C.msg_marshal_json(m.ptr)

	var buf []byte
	var sh = (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	sh.Cap, sh.Len, sh.Data = int(bufC.cap), int(bufC.len), uintptr(unsafe.Pointer(bufC.ptr))
	var n, err = b.Write(buf)

	C.buffer_drop(bufC)
	return n, err
}

func (m Message) UnmarshalJSON([]byte) error {

}

// Drop the Message, releasing underlying resources.
func (m Message) Drop() {
	C.msg_drop(m.ptr)
}
