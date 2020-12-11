package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// Extractor extracts UUIDs and packed field tuples from Documents.
type Extractor struct {
	svc    *service
	uuids  []pf.UUIDParts
	tuples [][]byte
	docs   int
}

// NewExtractor returns an instance of the Extractor service.
func NewExtractor(uuidPtr string, fieldPtrs []string) (*Extractor, error) {
	var svc = newService(
		func() *C.Channel { return C.extractor_create() },
		func(ch *C.Channel, in C.In1) { C.extractor_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.extractor_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.extractor_invoke16(ch, in) },
		func(ch *C.Channel) { C.extractor_drop(ch) },
	)

	var err = svc.sendMessage(0, &pf.ExtractRequest{UuidPtr: uuidPtr, FieldPtrs: fieldPtrs})
	if err != nil {
		return nil, err
	} else if _, _, err = svc.poll(); err != nil {
		return nil, err
	}

	return &Extractor{
		svc:    svc,
		uuids:  make([]pf.UUIDParts, 8),
		tuples: make([][]byte, 8),
		docs:   0,
	}, nil
}

// Document queues a document for extraction.
func (e *Extractor) Document(doc []byte) {
	e.svc.sendBytes(1, doc)
	e.docs++
}

// Extract UUIDs and field tuples from all documents queued since the last Extract.
// The returned UUIDParts and tuples are valid *only* until the next
// call to Extract -- you *must* copy out before calling Extract again.
func (e *Extractor) Extract() ([]pf.UUIDParts, [][]byte, error) {
	var _, out, err = e.svc.poll()
	if err != nil {
		return nil, nil, err
	}

	// Sanity check we got two output frames per document, as we expect.
	if len(out) != e.docs*2 {
		panic(fmt.Sprintf("wrong number of output frames (%d != %d * 2)", len(out), e.docs))
	}
	e.docs = 0

	e.uuids = e.uuids[:0]
	e.tuples = e.tuples[:0]

	for _, o := range out {
		if o.code == 0 {
			var uuid pf.UUIDParts
			e.svc.arena_decode(o, &uuid)
			e.uuids = append(e.uuids, uuid)
		} else {
			e.tuples = append(e.tuples, e.svc.arena_slice(o))
		}
	}
	return e.uuids, e.tuples, nil
}
