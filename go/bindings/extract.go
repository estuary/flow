package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	pf "github.com/estuary/flow/go/protocol"
)

type Extractor struct {
	svc    *service
	uuids  []pf.UUIDParts
	fields []pf.Field
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

	var fields = make([]pf.Field, len(fieldPtrs))

	return &Extractor{
		svc:    svc,
		uuids:  make([]pf.UUIDParts, 8),
		fields: fields,
	}, nil
}

// Document queues a document for extraction.
func (e *Extractor) Document(doc []byte) { e.svc.sendBytes(1, doc) }

// Extract UUIDs and Fields from all documents queued since the last Extract.
// The returned Arena, UUIDParts, and Fields are valid *only* until the next
// call to Extract -- you *must* copy any []bytes referenced by a Field out
// of Arena, before calling Extract again.
func (e *Extractor) Extract() (pf.Arena, []pf.UUIDParts, []pf.Field, error) {
	var arena, out, err = e.svc.poll()
	if err != nil {
		return nil, nil, nil, err
	}

	e.uuids = e.uuids[:0]
	for f := range e.fields {
		e.fields[f].Values = e.fields[f].Values[:0]
	}

	for _, o := range out {
		if o.code == 0 {
			var uuid pf.UUIDParts
			e.svc.arena_decode(o, &uuid)
			e.uuids = append(e.uuids, uuid)
		} else {
			var values = &e.fields[o.code-1].Values
			var val pf.Field_Value
			e.svc.arena_decode(o, &val)
			*values = append(*values, val)
		}
	}

	return arena, e.uuids, e.fields, nil
}
