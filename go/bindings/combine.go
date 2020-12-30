package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/estuary/flow/go/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// CombineBuilder builds Combine instances.
type CombineBuilder struct {
	// indexMemPtr is a pointer to a Rust allocated, leaked,
	// &'static SchemaIndex of a catalog.
	indexMemPtr uint64
	// pool holds initialized but available Combine instances.
	pool sync.Pool
}

// NewCombineBuilder initializes a new CombineBuilder,
// for building Combine instances using the given catalog.
func NewCombineBuilder(catalogPath string) (*CombineBuilder, error) {
	// Intern the |catalogPath| and receive a 'static SchemaIndex pointer.
	// We'll pass this to future Combiner instances.
	var svc = newCombineSvc()
	svc.sendBytes(0, []byte(catalogPath))

	var _, out, err = svc.poll()
	if err != nil {
		return nil, err
	}

	var cfg pf.CombineAPI_Config
	svc.arenaDecode(out[0], &cfg)

	return &CombineBuilder{
		indexMemPtr: cfg.SchemaIndexMemptr,
		pool: sync.Pool{
			New: func() interface{} { return newCombineSvc() },
		},
	}, nil
}

// Combine manages the lifecycle of a combine operation.
type Combine struct {
	svc  *service
	docs int
	out  []C.Out
}

// Open a new Combiner RPC, returning a ready Combiner instance to which documents may be Added.
func (f *CombineBuilder) Open(
	schemaURI string,
	keyPtrs []string,
	fieldPtrs []string,
	uuidPtr string,
	prune bool,
) (*Combine, error) {
	var svc = f.pool.Get().(*service)

	svc.mustSendMessage(1, &pf.CombineAPI_Config{
		SchemaIndexMemptr:  f.indexMemPtr,
		SchemaUri:          schemaURI,
		KeyPtr:             keyPtrs,
		FieldPtrs:          fieldPtrs,
		UuidPlaceholderPtr: uuidPtr,
		Prune:              prune,
	})
	var _, _, err = svc.poll()
	if err != nil {
		return nil, err
	}

	return &Combine{
		svc:  svc,
		docs: 0,
	}, nil
}

// Add |doc| to the Combine over the argument document.
func (c *Combine) Add(doc json.RawMessage) error {
	c.svc.sendBytes(2, doc)
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

	c.svc.sendBytes(3, nil)
	var _, out, err = c.svc.poll()
	if err != nil {
		return err
	}

	c.out = out
	return nil
}

// Finish combining documents, invoking the callback for each distinct group-by document.
func (c *Combine) Finish(cb func(json.RawMessage, []byte, tuple.Tuple) error) error {
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
	cb func(json.RawMessage, []byte, tuple.Tuple) error,
) error {
	// Sanity check we got triples of output frames.
	if len(*out)%3 != 0 {
		panic(fmt.Sprintf("wrong number of output frames (%d; should be %% 3)", len(*out)))
	}

	for len(*out) >= 3 {
		var doc = svc.arenaSlice((*out)[0])
		var key = svc.arenaSlice((*out)[1])
		var fields, err = tuple.Unpack(svc.arenaSlice((*out)[2]))

		if err != nil {
			panic(err) // Unexpected Rust <=> Go protocol error.
		} else if err = cb(doc, key, fields); err != nil {
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
