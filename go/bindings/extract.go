package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"fmt"
	"runtime"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// Extractor extracts UUIDs and packed field tuples from Documents.
type Extractor struct {
	svc         *service
	uuids       []pf.UUIDParts
	tuples      [][]byte
	docs        int
	pinnedIndex *SchemaIndex // Used from Rust.
}

// NewExtractor returns an instance of the Extractor service.
func NewExtractor() *Extractor {
	var extractor = &Extractor{
		svc:         newExtractSvc(),
		uuids:       make([]pf.UUIDParts, 32),
		tuples:      make([][]byte, 32),
		docs:        0,
		pinnedIndex: nil,
	}

	// Destroy the held service on collection.
	runtime.SetFinalizer(extractor, func(e *Extractor) {
		e.svc.destroy()
	})
	return extractor
}

// Configure or re-configure the Extractor. If schemaURI is non-empty, it's
// validated during extraction and the SchemaIndex must be non-nil.
// Otherwise, both may be zero-valued.
func (e *Extractor) Configure(
	uuidPtr string,
	fieldPtrs []string,
	schemaURI string,
	index *SchemaIndex,
) error {

	var schemaIndexMemptr uint64
	if schemaURI != "" {
		schemaIndexMemptr = index.indexMemPtr
	}
	e.pinnedIndex = index

	e.svc.mustSendMessage(
		uint32(pf.ExtractAPI_CONFIGURE),
		&pf.ExtractAPI_Config{
			UuidPtr:           uuidPtr,
			SchemaUri:         schemaURI,
			SchemaIndexMemptr: schemaIndexMemptr,
			FieldPtrs:         fieldPtrs,
		})

	return pollExpectNoOutput(e.svc)
}

// Document queues a document for extraction.
func (e *Extractor) Document(doc []byte) {
	e.svc.sendBytes(uint32(pf.ExtractAPI_EXTRACT), doc)
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
		if pf.ExtractAPI_Code(o.code) == pf.ExtractAPI_EXTRACTED_UUID {
			var uuid pf.UUIDParts
			e.svc.arenaDecode(o, &uuid)
			e.uuids = append(e.uuids, uuid)
		} else {
			e.tuples = append(e.tuples, e.svc.arenaSlice(o))
		}
	}
	return e.uuids, e.tuples, nil
}

func newExtractSvc() *service {
	return newService(
		func() *C.Channel { return C.extract_create() },
		func(ch *C.Channel, in C.In1) { C.extract_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.extract_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.extract_invoke16(ch, in) },
		func(ch *C.Channel) { C.extract_drop(ch) },
	)
}
