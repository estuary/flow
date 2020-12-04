package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	pf "github.com/estuary/flow/go/protocol"
)

type Extractor struct {
	svc    *Service
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

	var err = svc.SendMessage(0, &pf.ExtractRequest{UuidPtr: uuidPtr, FieldPtrs: fieldPtrs})
	if err != nil {
		return nil, err
	} else if _, _, err = svc.Poll(); err != nil {
		return nil, err
	}

	var fields = make([]pf.Field, len(fieldPtrs))

	return &Extractor{
		svc:    svc,
		uuids:  make([]pf.UUIDParts, 8),
		fields: fields,
	}, nil
}

// SendDocument sends the document to the Extractor.
func (e *Extractor) SendDocument(doc []byte) { e.svc.SendBytes(1, doc) }

// Poll the extractor for extracted UUIDs and Fields of all documents
// sent since the last Poll. The returned Arena, UUIDParts, and Fields
// are valid only until the next call to Poll.
func (e *Extractor) Poll() (pf.Arena, []pf.UUIDParts, []pf.Field, error) {
	var arena, frames, err = e.svc.Poll()
	if err != nil {
		return nil, nil, nil, err
	}

	e.uuids = e.uuids[:0]
	for f := range e.fields {
		e.fields[f].Values = e.fields[f].Values[:0]
	}

	// One frame for each UUID & field of every document.
	for len(frames) > len(e.fields) {

		var uuid pf.UUIDParts
		frames[0].MustDecode(&uuid)
		e.uuids = append(e.uuids, uuid)

		for f := 0; f != len(e.fields); f++ {
			var val pf.Field_Value
			frames[f+1].MustDecode(&val)
			e.fields[f].Values = append(e.fields[f].Values, val)
		}

		frames = frames[len(e.fields)+1:]
	}

	return arena, e.uuids, e.fields, nil
}
