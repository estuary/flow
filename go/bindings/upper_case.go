package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"reflect"
	"unsafe"
)

// UpperCase is a testing Service that upper-cases each input Frame,
// and returns the running sum length of its inputs via its response
// Frame Code.

func newUpperCase() *Service {
	return newService(
		func() *C.Channel { return C.upper_case_create() },
		func(ch *C.Channel, in C.In1) { C.upper_case_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.upper_case_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.upper_case_invoke16(ch, in) },
		func(ch *C.Channel) { C.upper_case_drop(ch) },
	)
}

// upperCaseNaieve mimics the invocation pattern of the upper-case service,
// taking a code & []byte payload, and returning a code and owned []byte
// payload. It does this unsafely via static Rust storage, but is safe for
// single-threaded test benchmarking.
func upperCaseNaieve(codeIn uint32, input []byte) (uint32, []byte) {
	var h = (*reflect.SliceHeader)(unsafe.Pointer(&input))
	var out_len C.uint32_t
	var out_ptr *C.uint8_t

	var codeOut = C.upper_case_naive(
		C.uint32_t(codeIn),
		(*C.uint8_t)(unsafe.Pointer(h.Data)),
		C.uint32_t(h.Len),
		&out_ptr,
		&out_len)

	var output []byte
	h = (*reflect.SliceHeader)(unsafe.Pointer(&output))
	h.Cap = int(out_len)
	h.Len = int(out_len)
	h.Data = uintptr(unsafe.Pointer(out_ptr))

	return uint32(codeOut), output
}
