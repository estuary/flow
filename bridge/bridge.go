package bridge

// #cgo LDFLAGS: -lestuary_bridge -lm -ldl -L ../target/debug/
/*
#include "bindings.h"
*/
import "C"
import (
	"bufio"
	"errors"
	"sync"
	"unsafe"

	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// JSONPointer wraps the native est_json_ptr_t.
type JSONPointer struct {
	wrapped *C.est_json_ptr_t
}

// NewJSONPointer constructs a new, parsed JSONPointer.
func NewJSONPointer(ptr string) (JSONPointer, error) {
	var ptrC = C.CString(ptr)
	defer C.free(unsafe.Pointer(ptrC))

	var out *C.est_json_ptr_t
	if s := C.est_json_ptr_new(ptrC, &out); s != C.EST_OK {
		return JSONPointer{}, statusError(s)
	}
	return JSONPointer{wrapped: out}, nil
}

// MustJSONPointer parses the given JSONPointer and panics if an error occurs.
func MustJSONPointer(ptr string) JSONPointer {
	if o, err := NewJSONPointer(ptr); err != nil {
		panic(err)
	} else {
		return o
	}
}

// Drop the JSONPointer, releasing underlying resources.
func (p JSONPointer) Drop() {
	C.est_json_ptr_drop(p.wrapped)
}

// Message wraps the native est_msg_t.
type Message struct {
	wrapped *C.est_msg_t
}

// NewMessage returns a new, empty Message with the given pointer.
func NewMessage(p JSONPointer) Message {
	return Message{wrapped: C.est_msg_new(p.wrapped)}
}

// GetUUID returns the UUID of the Message.
func (m Message) GetUUID() message.UUID {
	var uuidC = C.est_msg_get_uuid(m.wrapped)
	// Note RFC 4122 defines that UUIDs are always network byte order (big endian).
	return message.UUID(*(*[16]byte)((unsafe.Pointer)(&uuidC.bytes[0])))
}

// SetUUID updates the UUID of the Message.
func (m Message) SetUUID(uuid message.UUID) {
	var uuidC C.est_uuid_t
	*(*[16]byte)((unsafe.Pointer)(&uuidC.bytes[0])) = uuid
	C.est_msg_set_uuid(m.wrapped, uuidC)
}

// NewAcknowledgement returns a JSON document with an initialized UUID
// location, but which is otherwise empty.
func (m Message) NewAcknowledgement(protocol.Journal) message.Message {
	return Message{wrapped: C.est_msg_new_acknowledgement(m.wrapped)}
}

// FieldVisitor visits ordered field locations of a Message's JSON document.
// It's used with the VisitFields method.
type FieldVisitor interface {
	VisitDoesNotExist(index int)
	VisitNull(index int)
	VisitBool(index int, value bool)
	VisitUnsigned(index int, value uint64)
	VisitSigned(index int, value int64)
	VisitFloat(index int, value float64)
	VisitString(index int, value []byte)
	VisitObject(index int, encoded []byte)
	VisitArray(index int, encoded []byte)
}

// VisitFields invokes the FieldVisitor for each ordered JSONPointer.
func (m Message) VisitFields(fv FieldVisitor, ptrs ...JSONPointer) {
	if len(ptrs) == 0 {
		return
	}

	var fields = make([]C.est_extract_field_t, len(ptrs))
	for i, ptr := range ptrs {
		fields[i].ptr = ptr.wrapped
	}

	var b = bufferPool.Get().([]byte)
	for {
		var delta = int(C.est_msg_extract_fields(m.wrapped,
			&fields[0], (C.uintptr_t)(len(fields)),
			(*C.uint8_t)(&b[0]), (C.uintptr_t)(cap(b))))

		if delta > cap(b) {
			// Must re-allocate and try again.
			b = make([]byte, roundUp(int(delta)))
			continue
		}
		b = b[:delta]
		break
	}

	for i, field := range fields {
		switch field.type_ {
		case C.EST_DOES_NOT_EXIST:
			fv.VisitDoesNotExist(i)
		case C.EST_NULL:
			fv.VisitNull(i)
		case C.EST_TRUE:
			fv.VisitBool(i, true)
		case C.EST_FALSE:
			fv.VisitBool(i, false)
		case C.EST_UNSIGNED:
			fv.VisitUnsigned(i, uint64(field.unsigned_))
		case C.EST_SIGNED:
			fv.VisitSigned(i, int64(field.signed_))
		case C.EST_FLOAT:
			fv.VisitFloat(i, float64(field.float_))
		case C.EST_STRING:
			fv.VisitString(i, b[field.begin:field.end])
		case C.EST_OBJECT:
			fv.VisitObject(i, b[field.begin:field.end])
		case C.EST_ARRAY:
			fv.VisitArray(i, b[field.begin:field.end])
		}
	}
	bufferPool.Put(b)
}

// HashFields produces a combined, stable, deep hash of the values at
// the given document locations.
func (m Message) HashFields(ptrs ...JSONPointer) uint64 {
	if len(ptrs) == 0 {
		return 0
	}
	var hash = C.est_msg_hash_fields(m.wrapped,
		(**C.est_json_ptr_t)((unsafe.Pointer)(&ptrs[0])), (C.uintptr_t)(len(ptrs)))
	return uint64(hash)
}

// AppendJSONTo appends the JSON serialization of the message to the buffer.
// If the buffer is too small, it's re-allocated and copied to the next rounded-up
// power of two of sufficient size. The resulting appended-to buffer is returned.
func (m Message) AppendJSONTo(b []byte) []byte {
	var delta = 1
	for {
		var rem = cap(b) - len(b)
		if rem < delta {
			var next = make([]byte, len(b), roundUp(len(b)+delta))
			b = next[:copy(next, b)]
			continue
		}

		// Address of next byte to be written into |b|.
		var addr = &b[len(b) : len(b)+1][0]
		// Directly marshal into the remainder of |b|.
		delta = int(C.est_msg_marshal_json(m.wrapped,
			(*C.uint8_t)(addr), (C.uintptr_t)(rem)))

		if delta <= rem {
			return b[:len(b)+delta]
		}
	}
}

// MarshalJSONTo marshals the JSON message to the Writer.
func (m Message) MarshalJSONTo(bw *bufio.Writer) (int, error) {
	var b = bufferPool.Get().([]byte)
	b = m.AppendJSONTo(b[:0])
	var n, err = bw.Write(b)
	bufferPool.Put(b[:0])
	return n, err
}

// UnmarshalJSON unmarshals the Message from JSON.
func (m Message) UnmarshalJSON(buf []byte) error {
	var status = C.est_msg_unmarshal_json(m.wrapped,
		(*C.uint8_t)(&buf[0]), (C.uintptr_t)(len(buf)))

	if status != C.EST_OK {
		return statusError(status)
	}
	return nil
}

// Drop the Message, releasing underlying resources.
func (m Message) Drop() {
	C.est_msg_drop(m.wrapped)
}

func roundUp(n int) int {
	var count = 0
	for n != 0 {
		n = n >> 1
		count++
	}
	return 1 << count
}

var bufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0, 1024) },
}

func statusError(s C.est_status_t) error {
	var buf [64]byte
	var l = C.est_status_description(s, (*C.uint8_t)(&buf[0]), (C.uintptr_t)(len(buf)))
	return errors.New(string(buf[:l]))
}
