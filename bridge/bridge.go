package bridge

// #cgo LDFLAGS: -lestuary_bridge -ldl -L ../target/debug/
/*
#include "bindings.h"

Slice_c_char GoStringAsSlice(_GoString_ s) {
	Slice_c_char result = {
		pointer: _GoStringPtr(s),
		length: _GoStringLen(s),
	};
	return result;
}
*/
import "C"
import (
	"errors"
	"fmt"
)

// Message wraps the native Message.
type Message struct {
	msg *C.Message
}

// NewMessage returns a new message.
func NewMessage(content string, other int) (Message, error) {
	var slice = C.GoStringAsSlice(content)
	fmt.Printf("content %#v\n", slice)
	fmt.Printf("Go ssize %v\n", C.sizeof_Slice_c_char)

	var result = C.message_new(slice, C.int(other))
	if result.tag != C.Result_Ok {
		return Message{}, errors.New("foobar")
	}
	return Message{msg: C.result_message(&result)}, nil
}

// Length returns the message length.
func (m Message) Length() int {
	return int(C.message_length(m.msg))
}

// Extend extends the message.
func (m Message) Extend() {
	C.message_extend(m.msg)
}

// Free frees the Message, releasing underlying resources.
func (m Message) Free() {
	C.message_free(m.msg)
}
