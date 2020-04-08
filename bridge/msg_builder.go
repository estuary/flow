package bridge

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/estuary/proj/labels"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// MsgBuilder is an efficient builder of Message instances.
type MsgBuilder struct {
	// val is an instance of *map[uintptr]JSONPointer{}. We use atomic's
	// Load/Store/ComparePointer primitives because atomic.Value
	// doesn't allow for compare-and-swap.
	val unsafe.Pointer
}

// NewMsgBuilder returns a new MsgBuilder instance.
func NewMsgBuilder() *MsgBuilder {
	var b = new(MsgBuilder)
	atomic.StorePointer(&b.val, unsafe.Pointer(&map[uintptr]JSONPointer{}))
	return b
}

// Build a new Message instance, with a UUID pointer drawn from the labels.UUID
// field of the JournalSpec. Build is a message.NewMessageFunc and it is safe
// to call concurrently with *different* values of |spec| (but not for the
// same spec).
func (b *MsgBuilder) Build(spec *pb.JournalSpec) (message.Message, error) {
	var key = (uintptr)(unsafe.Pointer(spec))
	var m = (*map[uintptr]JSONPointer)(atomic.LoadPointer(&b.val))

	if ptr, ok := (*m)[key]; ok {
		return NewMessage(ptr), nil // Fast path.
	}

	// Slow path: we must allocate a new JSONPointer and update the map.
	var uuidStr = spec.LabelSet.ValueOf(labels.UUID)
	if uuidStr == "" {
		return nil, fmt.Errorf("journal %q: missing required %q label", spec.Name, labels.UUID)
	}
	var insert, err = NewJSONPointer(uuidStr)
	if err != nil {
		return nil, fmt.Errorf("journal %q: invalid %q label %q: %w", spec.Name, labels.UUID, uuidStr, err)
	}

	for {
		// Grab latest version of map (ie assume we lost an insertion race).
		m = (*map[uintptr]JSONPointer)(atomic.LoadPointer(&b.val))
		if _, ok := (*m)[key]; ok {
			panic("concurrent call to Build for same *pb.JournalSpec")
		}
		// Rebuild a new map with our inserted value.
		var next = make(map[uintptr]JSONPointer, len(*m)+1)
		for k, v := range *m {
			next[k] = v
		}
		next[key] = insert

		if atomic.CompareAndSwapPointer(&b.val, unsafe.Pointer(m), unsafe.Pointer(&next)) {
			return NewMessage(insert), nil
		}
	}
}

// PurgeCache of the MsgBuilder, clearing its internal cache and freeing
// underlying resources. PurgeCache may be called concurrently with Build.
func (b *MsgBuilder) PurgeCache() {
	var m *map[uintptr]JSONPointer
	var n = &map[uintptr]JSONPointer{}

	// Loop until we atomically replace |m| with an empty map.
	for {
		m = (*map[uintptr]JSONPointer)(atomic.LoadPointer(&b.val))
		if atomic.CompareAndSwapPointer(&b.val, unsafe.Pointer(m), unsafe.Pointer(n)) {
			break
		}
	}
	for _, v := range *m {
		v.Drop()
	}
}
