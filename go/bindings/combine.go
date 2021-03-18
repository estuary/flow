package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"runtime"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// CombineCallback is the callback accepted by Combine.Finish and Derive.Finish.
type CombineCallback = func(
	// Is this document fully reduced (it included a ReduceLeft operation),
	// or only partially reduced (from CombineRight operations only)?
	full bool,
	// Encoded JSON document, with a UUID placeholder if that was requested.
	doc json.RawMessage,
	// Packed tuple.Tuple of the document key.
	packedKey []byte,
	// Packed tuple.Tuple of requested location pointers.
	packedFields []byte,
) error

// Combine manages the lifecycle of a combiner operation.
type Combine struct {
	svc         *service
	drained     []C.Out
	pinnedIndex *SchemaIndex // Used from Rust.
}

// NewCombiner builds and returns a new Combine.
func NewCombine(
	index *SchemaIndex,
	schemaURI string,
	keyPtrs []string,
	fieldPtrs []string,
	uuidPtr string,
) (*Combine, error) {
	var svc = newCombineSvc()

	svc.mustSendMessage(
		uint32(pf.CombineAPI_CONFIGURE),
		&pf.CombineAPI_Config{
			SchemaIndexMemptr:  index.indexMemPtr,
			SchemaUri:          schemaURI,
			KeyPtr:             keyPtrs,
			FieldPtrs:          fieldPtrs,
			UuidPlaceholderPtr: uuidPtr,
		})
	var _, _, err = svc.poll()
	if err != nil {
		svc.destroy()
		return nil, err
	}

	var combine = &Combine{
		svc:         svc,
		drained:     nil,
		pinnedIndex: index,
	}

	// Destroy the held service on collection.
	runtime.SetFinalizer(combine, func(c *Combine) {
		c.svc.destroy()
	})

	return combine, nil
}

// ReduceLeft reduces |doc| as a fully reduced, left-hand document.
func (c *Combine) ReduceLeft(doc json.RawMessage) error {
	c.drained = nil // Invalidate.
	c.svc.sendBytes(uint32(pf.CombineAPI_REDUCE_LEFT), doc)

	if c.svc.queuedFrames() >= 128 {
		return pollExpectNoOutput(c.svc)
	}
	return nil
}

// CombineRight combines |doc| as a partially reduced, right-hand document.
func (c *Combine) CombineRight(doc json.RawMessage) error {
	c.drained = nil // Invalidate.
	c.svc.sendBytes(uint32(pf.CombineAPI_COMBINE_RIGHT), doc)

	if c.svc.queuedFrames() >= 128 {
		return pollExpectNoOutput(c.svc)
	}
	return nil
}

// PrepareToDrain the Combine by flushing any unsent documents to combine,
// and staging combined results into the Combine's service arena.
// Any validation or reduction errors in input documents will be surfaced
// prior to the return of this call.
// Preparing to drain is optional: it will be done by Drain if not already prepared.
func (c *Combine) PrepareToDrain() error {
	c.svc.sendBytes(uint32(pf.CombineAPI_DRAIN), nil)
	var _, out, err = c.svc.poll()
	if err != nil {
		return err
	}
	c.drained = out
	return nil
}

// Drain combined documents, invoking the callback for each distinct group-by document.
// If Drain returns without error, the Combine may be used again.
func (c *Combine) Drain(cb CombineCallback) error {
	if c.drained == nil {
		if err := c.PrepareToDrain(); err != nil {
			return err
		}
	}
	return drainCombineToCallback(c.svc, &c.drained, cb)
}

func drainCombineToCallback(
	svc *service,
	out *[]C.Out,
	cb CombineCallback,
) error {
	// Sanity check we got triples of output frames.
	if len(*out)%3 != 0 {
		panic(fmt.Sprintf("wrong number of output frames (%d; should be %% 3)", len(*out)))
	}

	for len(*out) >= 3 {
		if err := cb(
			pf.CombineAPI_Code((*out)[0].code) == pf.CombineAPI_DRAINED_REDUCED_DOCUMENT,
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

func newCombineSvc() *service {
	return newService(
		func() *C.Channel { return C.combine_create() },
		func(ch *C.Channel, in C.In1) { C.combine_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.combine_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.combine_invoke16(ch, in) },
		func(ch *C.Channel) { C.combine_drop(ch) },
	)
}
