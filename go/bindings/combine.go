package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"sync"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// CombineBuilder builds Combine instances.
type CombineBuilder struct {
	index *SchemaIndex
	// pool holds initialized but available Combine instances.
	pool sync.Pool
}

// NewCombineBuilder initializes a new CombineBuilder,
// for building Combine instances using the given catalog.
func NewCombineBuilder(index *SchemaIndex) *CombineBuilder {
	return &CombineBuilder{
		index: index,
		pool: sync.Pool{
			New: func() interface{} { return newCombineSvc() },
		},
	}
}

// Combine manages the lifecycle of a combine operation.
type Combine struct {
	svc         *service
	docs        int
	out         []C.Out
	pinnedIndex *SchemaIndex
}

// Open a new Combiner RPC, returning a ready Combiner instance to which documents may be Added.
func (f *CombineBuilder) Open(
	schemaURI string,
	keyPtrs []string,
	fieldPtrs []string,
	uuidPtr string,
) (*Combine, error) {
	var svc = f.pool.Get().(*service)

	svc.mustSendMessage(1, &pf.CombineAPI_Config{
		SchemaIndexMemptr:  f.index.indexMemPtr,
		SchemaUri:          schemaURI,
		KeyPtr:             keyPtrs,
		FieldPtrs:          fieldPtrs,
		UuidPlaceholderPtr: uuidPtr,
	})
	var _, _, err = svc.poll()
	if err != nil {
		return nil, err
	}

	return &Combine{
		svc:         svc,
		docs:        0,
		pinnedIndex: f.index,
	}, nil
}

// ReduceLeft reduces |doc| as a fully reduced, left-hand document.
func (c *Combine) ReduceLeft(doc json.RawMessage) error {
	c.svc.sendBytes(2, doc)
	c.docs++

	var err error
	if c.docs%128 == 0 {
		err = c.Flush()
	}
	return err
}

// CombineRight combines |doc| as a partially reduced, right-hand document.
func (c *Combine) CombineRight(doc json.RawMessage) error {
	c.svc.sendBytes(3, doc)
	c.docs++

	var err error
	if c.docs%128 == 0 {
		err = c.Flush()
	}
	return err
}

// Flush documents which haven't yet been submitted.
func (c *Combine) Flush() error {
	if _, out, err := c.svc.poll(); err != nil {
		return err
	} else if len(out) != 0 {
		panic("unexpected output frames")
	}
	return nil
}

// CloseSend closes the Combine stream from further added documents,
// and prepares combined outputs for a future Finish() call.
func (c *Combine) CloseSend() error {
	if c.out != nil {
		return nil // Already called.
	}

	c.svc.sendBytes(4, nil)
	var _, out, err = c.svc.poll()
	if err != nil {
		return err
	}

	c.out = out
	return nil
}

// Finish combining documents, invoking the callback for each distinct group-by document.
func (c *Combine) Finish(cb func(doc json.RawMessage, packedKey, packedFields []byte) error) error {
	if err := c.CloseSend(); err != nil {
		return err
	} else if err := drainCombineToCallback(c.svc, &c.out, cb); err != nil {
		return err
	}

	c.docs = 0
	return nil
}

func drainCombineToCallback(
	svc *service,
	out *[]C.Out,
	cb func(doc json.RawMessage, packedKey, packedFields []byte) error,
) error {
	// Sanity check we got triples of output frames.
	if len(*out)%3 != 0 {
		panic(fmt.Sprintf("wrong number of output frames (%d; should be %% 3)", len(*out)))
	}

	for len(*out) >= 3 {
		if err := cb(
			svc.arenaSlice((*out)[0]), // Doc.
			svc.arenaSlice((*out)[1]), // Packed key.
			svc.arenaSlice((*out)[2]), // Packed fields.
		); err != nil {
			return err
		}
		*out = (*out)[3:]
	}

	return nil
}

// Release a Combiner which has been Drained without error.
func (f *CombineBuilder) Release(c *Combine) { f.pool.Put(c.svc) }

func newCombineSvc() *service {
	return newService(
		func() *C.Channel { return C.combine_create() },
		func(ch *C.Channel, in C.In1) { C.combine_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.combine_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.combine_invoke16(ch, in) },
		func(ch *C.Channel) { C.combine_drop(ch) },
	)
}
