package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	pf "github.com/estuary/flow/go/protocols/flow"
)

// SchemaIndex wraps a compiled JSON schema index allocated in Rust memory,
// that's able to be shared with other Rust APIs.
type SchemaIndex struct {
	indexMemPtr uint64
}

// NewSchemaIndex builds and indexes the provided bundle of schemas.
func NewSchemaIndex(bundle *pf.SchemaBundle) (*SchemaIndex, error) {
	var svc = newSchemaService()
	defer svc.destroy()

	if err := svc.sendMessage(1, bundle); err != nil {
		panic(err) // Encoding is infalliable.
	}

	var _, out, err = svc.poll()
	if err != nil {
		return nil, err
	}

	var built pf.SchemaAPI_BuiltIndex
	svc.arenaDecode(out[0], &built)

	return &SchemaIndex{
		indexMemPtr: built.SchemaIndexMemptr,
	}, nil
}

func newSchemaService() *service {
	return newService(
		func() *C.Channel { return C.schema_create() },
		func(ch *C.Channel, in C.In1) { C.schema_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.schema_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.schema_invoke16(ch, in) },
		func(ch *C.Channel) { C.schema_drop(ch) },
	)
}
