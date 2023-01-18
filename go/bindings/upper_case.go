package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"unsafe"

	"github.com/estuary/flow/go/ops"
)

// newUpperCase is a testing Service that upper-cases each input Frame,
// and returns the running sum length of its inputs via its response
// Frame Code.
func newUpperCase(publisher ops.Publisher) *service {
	var svc, err = newService(
		"uppercase",
		func(logFilter, logDest C.int32_t) *C.Channel { return C.upper_case_create(logFilter, logDest) },
		func(ch *C.Channel, in C.In1) { C.upper_case_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.upper_case_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.upper_case_invoke16(ch, in) },
		func(ch *C.Channel) { C.upper_case_drop(ch) },
		publisher,
	)
	if err != nil {
		panic(err)
	}
	return svc
}

// newNoOpService is a testing Service that doesn't invoke into CGO,
// but still produces an (empty) output Frame for each input.
func newNoOpService(publisher ops.Publisher) *service {
	var svc, err = newService(
		"noop",
		func(_, _ C.int32_t) *C.Channel {
			var ch = (*C.Channel)(C.calloc(C.sizeof_Channel, 1))
			ch.out_ptr = (*C.Out)(C.calloc(C.sizeof_Out, 512))

			ch.out_cap = 512
			ch.out_len = 0

			return ch
		},
		// Increment output cursors, so that we build Frames for each input,
		// but don't actually invoke CGO.
		func(ch *C.Channel, in C.In1) { ch.out_len++ },
		func(ch *C.Channel, in C.In4) { ch.out_len += 4 },
		func(ch *C.Channel, in C.In16) { ch.out_len += 16 },
		func(ch *C.Channel) {
			C.free(unsafe.Pointer(ch.out_ptr))
			C.free(unsafe.Pointer(ch))
		},
		publisher,
	)
	if err != nil {
		panic(err)
	}
	return svc
}

// upperCaseNaive is an alternative, non-Service implementation which
// does the same task using a more typical CGO invocation pattern.
// It too avoids copies and returned Rust-owned memory after each call.
// It's here for comparative benchmarking.
type upperCaseNaive struct {
	svc *C.ServiceImpl
}

func newUpperCaseNaive() upperCaseNaive {
	return upperCaseNaive{
		svc: C.create_upper_case_naive(),
	}
}

func (s *upperCaseNaive) invoke(codeIn uint32, input []byte) (uint32, []byte, error) {
	var h = (*reflect.SliceHeader)(unsafe.Pointer(&input))
	var outLen C.uint32_t
	var outPtr *C.uint8_t

	var codeOut = C.upper_case_naive(
		s.svc,
		C.uint32_t(codeIn),
		(*C.uint8_t)(unsafe.Pointer(h.Data)),
		C.uint32_t(h.Len),
		&outPtr,
		&outLen)

	var output []byte
	h = (*reflect.SliceHeader)(unsafe.Pointer(&output))
	h.Cap = int(outLen)
	h.Len = int(outLen)
	h.Data = uintptr(unsafe.Pointer(outPtr))

	var err error
	if codeOut == math.MaxUint32 {
		err = errors.New(string(output))
	}
	return uint32(codeOut), output, err
}

// upperCaseGo is yet another pure-Go implementation, for comparative benchmarking.
type upperCaseGo struct {
	invoke func(uint32, []byte) (uint32, []byte, error)
}

func newUpperCaseGo() upperCaseGo {
	var sumLength uint32
	var arena []byte

	// Use a closure to force dynamic dispatch / prevent inlining.
	var fn = func(codeIn uint32, input []byte) (uint32, []byte, error) {
		if bytes.Equal(input, []byte("whoops")) {
			return 0, nil, errors.New("whoops")
		}

		arena = append(arena[:0], input...)
		sumLength += uint32(len(input))

		for i, b := range arena {
			if b >= 'a' && b <= 'z' {
				arena[i] = b - 'a' + 'A'
			}
		}
		return sumLength, arena, nil
	}

	return upperCaseGo{invoke: fn}
}
