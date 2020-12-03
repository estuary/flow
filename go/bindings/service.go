package bindings

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/release -lbindings -ldl
#include "../../crates/bindings/flow_bindings.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"unsafe"
)

// Service is a Go handle to an instantiated service binding.
type Service struct {
	ch       *C.Channel
	frameIn  []C.In1
	frameOut []Frame
	frameBuf []byte

	invoke1  func(*C.Channel, C.In1)
	invoke4  func(*C.Channel, C.In4)
	invoke16 func(*C.Channel, C.In16)
}

// Build a new Service instance. This is to be wrapped by concrete, exported
// Service constructors of this package -- constructors which also handle
// bootstrap and configuration of the Service, map to returned errors, and may
// provide friendlier interfaces than those of Service.
func newService(
	create func() *C.Channel,
	invoke1 func(*C.Channel, C.In1),
	invoke4 func(*C.Channel, C.In4),
	invoke16 func(*C.Channel, C.In16),
	drop func(*C.Channel),
) *Service {
	var ch = create()

	var svc = &Service{
		ch:       ch,
		frameIn:  make([]C.In1, 0, 16),
		frameOut: make([]Frame, 0, 16),
		frameBuf: make([]byte, 0, 256),
		invoke1:  invoke1,
		invoke4:  invoke4,
		invoke16: invoke16,
	}
	runtime.SetFinalizer(svc, func(svc *Service) {
		drop(svc.ch)
	})

	return svc
}

// Frame is a payload which may be passed to and from a Service.
type Frame struct {
	// User-defined Code of the Frame.
	Code uint32
	// Data payload of the frame.
	Data []byte
}

// Frameable is the interface provided by messages that know how to frame
// themselves, notably Protobuf messages.
type Frameable interface {
	ProtoSize() int
	MarshalToSizedBuffer(dAtA []byte) (int, error)
}

// SendBytes to the Service.
// The sent |data| must not be changed until the next Service Poll().
func (s *Service) SendBytes(code uint32, data []byte) {
	var h = (*reflect.SliceHeader)(unsafe.Pointer(&data))

	s.frameIn = append(s.frameIn, C.In1{
		code:     C.uint32_t(code),
		data_len: C.uint32_t(h.Len),
		data_ptr: (*C.uint8_t)(unsafe.Pointer(h.Data)),
	})
}

// SendMessage sends the serialization of a Frameable message to the Service.
func (s *Service) SendMessage(code uint32, m Frameable) error {
	var n, err = m.MarshalToSizedBuffer(s.ReserveBytes(code, m.ProtoSize()))
	if err != nil {
		return err
	} else if n != 0 {
		return fmt.Errorf("MarshalToSizedBuffer left unexpected remainder: %d", n)
	}
	return nil
}

// ReserveBytes reserves a length-sized []byte slice which will be
// sent with the next Service Poll(). Until then, the caller may
// write into the returned bytes, e.x. in order to serialize a
// message of prior known size.
func (s *Service) ReserveBytes(code uint32, length int) []byte {
	var l = len(s.frameBuf)
	var c = cap(s.frameBuf)

	if c-l < length {
		// Grow frameBuf, but don't bother to copy (prior buffers are
		// still pinned by their current Frames).
		for c < length {
			c = c << 1
		}
		s.frameBuf, l = make([]byte, 0, c), 0
	}

	var next = s.frameBuf[0 : l+length]
	s.SendBytes(code, next[l:])
	s.frameBuf = next

	return next[l:]
}

// Poll the Service. On return, all frames sent since the last Poll have been
// processed, and any response Frames are returned. Poll also returns a memory
// arena which individual Frames may reference (e.x., by encoding offsets into
// the returned arena).
// NOTE: The []byte arena and returned Frame Data is owned by the Service, not Go,
// and is *ONLY* valid until the next call to Poll(). At that point, it may be
// over-written or freed, and attempts to access it may crash the program.
func (s *Service) Poll() ([]byte, []Frame, error) {
	// Reset output storage cursors.
	// SAFETY: the channel arena and output frames hold only integer types
	// (u8 bytes and u32 offsets, respectively), having trivial impl Drops.
	s.ch.arena_len = 0
	s.ch.out_len = 0

	var input = s.frameIn

	// Invoke in strides of 16.
	// The compiler is smart enough to omit bounds checks here.
	for len(input) >= 16 {
		s.invoke16(s.ch, C.In16{
			in0: C.In4{
				in0: input[0],
				in1: input[1],
				in2: input[2],
				in3: input[3],
			},
			in1: C.In4{
				in0: input[4],
				in1: input[5],
				in2: input[6],
				in3: input[7],
			},
			in2: C.In4{
				in0: input[8],
				in1: input[9],
				in2: input[10],
				in3: input[11],
			},
			in3: C.In4{
				in0: input[12],
				in1: input[13],
				in2: input[14],
				in3: input[15],
			},
		})
		input = input[16:]
	}
	// Invoke in strides of 4.
	for len(input) >= 4 {
		s.invoke4(s.ch, C.In4{
			in0: input[0],
			in1: input[1],
			in2: input[2],
			in3: input[3],
		})
		input = input[4:]
	}
	// Invoke in strides of 1.
	for _, in := range input {
		s.invoke1(s.ch, in)
	}
	// All inputs are consumed. Reset.
	s.frameIn = s.frameIn[:0]
	s.frameBuf = s.frameBuf[:0]

	// During invocations, ch.arena_*, ch.out_*, and ch.err_* slices were updated.
	// Obtain zero-copy access to each of them.
	var arena []byte
	var chOut []C.Out
	var chErr []byte

	var arenaHeader = (*reflect.SliceHeader)(unsafe.Pointer(&arena))
	var chOutHeader = (*reflect.SliceHeader)(unsafe.Pointer(&chOut))
	var chErrHeader = (*reflect.SliceHeader)(unsafe.Pointer(&chErr))

	arenaHeader.Cap = int(s.ch.arena_cap)
	arenaHeader.Len = int(s.ch.arena_len)
	arenaHeader.Data = uintptr(unsafe.Pointer(s.ch.arena_ptr))

	chOutHeader.Cap = int(s.ch.out_cap)
	chOutHeader.Len = int(s.ch.out_len)
	chOutHeader.Data = uintptr(unsafe.Pointer(s.ch.out_ptr))

	chErrHeader.Cap = int(s.ch.err_cap)
	chErrHeader.Len = int(s.ch.err_len)
	chErrHeader.Data = uintptr(unsafe.Pointer(s.ch.err_ptr))

	// We must copy raw C.Out instances to our Go-side |frameOut|.

	// First grow it, if required.
	if c := cap(s.frameOut); c < len(chOut) {
		for c < len(chOut) {
			c = c << 1
		}
		s.frameOut = make([]Frame, len(chOut), c)
	} else {
		s.frameOut = s.frameOut[:len(chOut)]
	}

	for i, o := range chOut {
		// This avoids the bounds check into |arena| which would otherwise be done,
		// if Go slicing were used. Equivalent to `arena[o.begin:o.end]`.
		var data []byte
		var dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&data))
		dataHeader.Cap = int(o.end - o.begin)
		dataHeader.Len = int(o.end - o.begin)
		dataHeader.Data = uintptr(unsafe.Pointer(s.ch.arena_ptr)) + uintptr(o.begin)

		s.frameOut[i] = Frame{
			Code: uint32(o.code),
			Data: data,
		}
	}

	var err error
	if len(chErr) != 0 {
		err = errors.New(string(chErr))
	}

	return arena, s.frameOut, err
}
