package bindings

/*
#include "../../crates/bindings/flow_bindings.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/estuary/flow/go/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

// service is a Go handle to an instantiated service binding.
type service struct {
	typeName string
	ch       *C.Channel
	in       []C.In1
	buf      []byte

	invoke1  func(*C.Channel, C.In1)
	invoke4  func(*C.Channel, C.In4)
	invoke16 func(*C.Channel, C.In16)
	drop     func(*C.Channel)
}

// newService builds a new service instance. This is to be wrapped by concrete,
// exported service constructors of this package -- constructors which also handle
// bootstrap and configuration of the service, map to returned errors, and provide
// memory-safe interfaces for interacting with the service.
func newService(
	typeName string,
	create func(C.int32_t, C.int32_t) *C.Channel,
	invoke1 func(*C.Channel, C.In1),
	invoke4 func(*C.Channel, C.In4),
	invoke16 func(*C.Channel, C.In16),
	drop func(*C.Channel),
	publisher ops.Publisher,
) (*service, error) {
	var logReader, wDescriptor, err = Pipe()
	if err != nil {
		return nil, fmt.Errorf("creating logging pipe: %w", err)
	}

	var ch = create(C.int32_t(publisher.Labels().LogLevel), C.int32_t(wDescriptor))
	// Rust services produce canonical JSON encodings of ops::Log into `wDescriptor`.
	// Parse each and pass to our `publisher`.
	go func() {
		var _, err = io.Copy(ops.NewLogWriteAdapter(publisher), logReader)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"error":   err,
				"service": typeName,
				"labels":  publisher.Labels(),
			}).Error("failed to process cgo service channel logs")
		}
	}()

	serviceCreatedCounter.WithLabelValues(typeName).Inc()
	var svc = &service{
		typeName: typeName,
		ch:       ch,
		in:       make([]C.In1, 0, 16),
		buf:      make([]byte, 0, 256),
		invoke1:  invoke1,
		invoke4:  invoke4,
		invoke16: invoke16,
		drop:     drop,
	}

	return svc, nil
}

// marshaler is a message that knows how to frame itself (e.x. protobuf messages).
type marshaler interface {
	ProtoSize() int
	MarshalToSizedBuffer([]byte) (int, error)
}

// unmarshaler is a message that knows how to unframe itself.
type unmarshaler interface {
	Unmarshal([]byte) error
}

// queuedFrames returns the number of frames queued to send over the
// service channel on the next poll().
func (s *service) queuedFrames() int {
	return len(s.in)
}

// sendBytes to the service.
// The sent |data| must not be changed until the next service poll().
func (s *service) sendBytes(code uint32, data []byte) {
	var h = (*reflect.SliceHeader)(unsafe.Pointer(&data))

	s.in = append(s.in, C.In1{
		code:     C.uint32_t(code),
		data_len: C.uint32_t(h.Len),
		data_ptr: (*C.uint8_t)(unsafe.Pointer(h.Data)),
	})
}

// sendMessage sends the serialization of Marshaler to the Service.
func (s *service) sendMessage(code uint32, m marshaler) error {
	var r = s.reserveBytes(code, m.ProtoSize())

	if n, err := m.MarshalToSizedBuffer(r); err != nil {
		return err
	} else if n != len(r) {
		return fmt.Errorf("MarshalToSizedBuffer left unexpected remainder: %d vs %d", n, len(r))
	}
	return nil
}

// mustSendMessage sends the serialization of Marshaler to the Service,
// and panics on a serialization error.
func (s *service) mustSendMessage(code uint32, m marshaler) {
	if err := s.sendMessage(code, m); err != nil {
		panic(err)
	}
}

// reserveBytes reserves a length-sized []byte slice which will be
// sent with the next service poll(). Until then, the caller may
// write into the returned bytes, e.x. in order to serialize a
// message of prior known size.
func (s *service) reserveBytes(code uint32, length int) []byte {
	var l = len(s.buf)
	var c = cap(s.buf)

	if c-l < length {
		// Grow frameBuf, but don't bother to copy
		// (prior buffers are still pinned by |s.in|).
		for c < length {
			c = c << 1
		}
		s.buf, l = make([]byte, 0, c), 0
	}

	var next = s.buf[0 : l+length]
	s.sendBytes(code, next[l:])
	s.buf = next

	return next[l:]
}

// poll the Service. On return, all inputs sent since the last poll() have been
// processed, and any response []C.Out's are returned with any error encountered.
// arena which individual Frames may reference (e.x., by encoding offsets into
// the returned arena).
// NOTE: The []byte arena and returned Frame Data is owned by the Service, not Go,
// and is *ONLY* valid until the next call to Poll(). At that point, it may be
// over-written or freed, and attempts to access it may crash the program.
func (s *service) poll() (pf.Arena, []C.Out, error) {
	// Reset output storage cursors.
	// SAFETY: the channel arena and output frames hold only integer types
	// (u8 bytes and u32 offsets, respectively), having trivial impl Drops.
	s.ch.arena_len = 0
	s.ch.out_len = 0

	var input = s.in

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
	s.in = s.in[:0]
	s.buf = s.buf[:0]

	// During invocations, ch.arena_*, ch.out_*, and ch.err_* slices were updated.
	// Obtain zero-copy access to each of them.
	var arena pf.Arena
	var chOut []C.Out

	var arenaHeader = (*reflect.SliceHeader)(unsafe.Pointer(&arena))
	var chOutHeader = (*reflect.SliceHeader)(unsafe.Pointer(&chOut))

	arenaHeader.Cap = int(s.ch.arena_cap)
	arenaHeader.Len = int(s.ch.arena_len)
	arenaHeader.Data = uintptr(unsafe.Pointer(s.ch.arena_ptr))

	chOutHeader.Cap = int(s.ch.out_cap)
	chOutHeader.Len = int(s.ch.out_len)
	chOutHeader.Data = uintptr(unsafe.Pointer(s.ch.out_ptr))

	// Check for and return a ch.err_*.
	var err error
	if s.ch.err_len != 0 {
		err = errors.New(C.GoStringN(
			(*C.char)(unsafe.Pointer(s.ch.err_ptr)),
			C.int(s.ch.err_len)))
	}

	return arena, chOut, err
}

// arenaSlice returns a []byte slice of the arena, using trusted offsets.
// It skips bounds checks which would otherwise be done.
// Equivalent to `arena()[from:to]`.
func (s *service) arenaSlice(o C.Out) (b []byte) {
	var h = (*reflect.SliceHeader)(unsafe.Pointer(&b))

	h.Cap = int(o.end - o.begin)
	h.Len = int(o.end - o.begin)
	h.Data = uintptr(unsafe.Pointer(s.ch.arena_ptr)) + uintptr(o.begin)

	return
}

// arenaDecode decodes the unmarshaler from the given trusted arena offsets.
func (s *service) arenaDecode(o C.Out, m unmarshaler) unmarshaler {
	if err := m.Unmarshal(s.arenaSlice(o)); err != nil {
		panic(err)
	}
	return m
}

// destroy the service, dropping its internal CGO channel and invoking
// Rust Drop behavior. This immediately invalidates any Rust memory
// being read from the Go side of the bridge!
// Users of service must call destroy to release the service, but must
// do so only after they can guarantee they hold no active references
// to Rust-owned memory.
func (s *service) destroy() {
	if s.ch != nil {
		s.drop(s.ch)
	}
	s.ch = nil
	serviceDestroyedCounter.WithLabelValues(s.typeName).Inc()
}

func pollExpectNoOutput(svc *service) error {
	if _, out, err := svc.poll(); err != nil {
		return err
	} else if len(out) != 0 {
		panic(fmt.Sprintf("unexpected output frames %#v", out))
	}
	return nil
}

var serviceDestroyedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_bindings_service_destroyed_total",
	Help: "Counter of bindings service instances destroyed",
}, []string{"type"})
var serviceCreatedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_bindings_service_created_total",
	Help: "Counter of bindings service instances created",
}, []string{"type"})
