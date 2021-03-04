package tester

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/fdb/tuple"
	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	"go.gazette.dev/core/message"
)

type testDocGenerator struct {
	docIndex   int
	valueIndex int
	spec       *pf.MaterializationSpec
	keys       []field
	values     []field
	uuidPtr    flow.Pointer
	producerID message.ProducerID
}

func newTestDocGenerator(spec *pf.MaterializationSpec) (*testDocGenerator, error) {
	var keys []field
	for _, keyField := range spec.FieldSelection.Keys {
		var projection = spec.Collection.GetProjection(keyField)
		var ptr, err = flow.NewPointer(projection.Ptr)
		if err != nil {
			return nil, fmt.Errorf("parsing key pointer: %w", err)
		}
		keys = append(keys, field{
			ptr:       &ptr,
			inference: projection.Inference,
		})
	}
	var values []field
	for _, valueField := range spec.FieldSelection.Values {
		var projection = spec.Collection.GetProjection(valueField)
		var ptr, err = flow.NewPointer(projection.Ptr)
		if err != nil {
			return nil, fmt.Errorf("parsing key pointer: %w", err)
		}
		values = append(values, field{
			ptr:       &ptr,
			inference: projection.Inference,
		})
	}
	uuidPtr, err := flow.NewPointer(spec.Shuffle.SourceUuidPtr)
	if err != nil {
		return nil, fmt.Errorf("creating uuid pointer: %w", err)
	}

	return &testDocGenerator{
		spec:       spec,
		keys:       keys,
		values:     values,
		uuidPtr:    uuidPtr,
		producerID: message.NewProducerID(),
		docIndex:   0,
	}, nil
}

type TestDoc struct {
	Key      tuple.Tuple
	Values   tuple.Tuple
	Document interface{}
	Exists   bool
}

func (d *TestDoc) docJson() json.RawMessage {
	var bytes, err = json.Marshal(d.Document)
	if err != nil {
		panic(fmt.Sprintf("marshalling test document json cannot fail: %v", err))
	}
	return bytes
}

func (g *testDocGenerator) setUUID(doc *interface{}) {
	var uuid = message.BuildUUID(g.producerID, message.NewClock(time.Now()), message.Flag_CONTINUE_TXN)
	var loc, err = g.uuidPtr.Create(doc)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize UUID location: %v", err))
	}
	*loc = uuid.String()
}

func (g *testDocGenerator) GenerateTestDoc() *TestDoc {
	g.docIndex++
	var doc interface{}
	var docKey tuple.Tuple
	var docValues tuple.Tuple
	g.setUUID(&doc)
	for _, key := range g.keys {
		var k = key.genValue(&doc, g.docIndex)
		docKey = append(docKey, k)
	}

	for _, field := range g.values {
		g.valueIndex++
		var v = field.genValue(&doc, g.valueIndex)
		docValues = append(docValues, v)
	}
	return &TestDoc{
		Key:      docKey,
		Values:   docValues,
		Document: doc,
	}
}

func (g *testDocGenerator) GenerateTestDocs(count int) []*TestDoc {
	var testDocs = make([]*TestDoc, count)
	for i := 0; i < count; i++ {
		testDocs[i] = g.GenerateTestDoc()
	}
	return testDocs
}

func (g *testDocGenerator) UpdateValues(doc *TestDoc) {
	g.setUUID(&doc.Document)
	for i, field := range g.values {
		g.valueIndex++
		var v = field.genValue(&doc.Document, g.valueIndex)
		doc.Values[i] = v
	}
}

type field struct {
	ptr       *flow.Pointer
	inference *pf.Inference
}

func (f field) genValue(doc *interface{}, index int) interface{} {
	var jsonType = f.inference.Types[int(index)%len(f.inference.Types)]
	var loc, err = f.ptr.Create(doc)
	if err != nil {
		// We panic here instead of returning the error because all fields of this test document are
		// intended to be generated, and thus no invalid parent types should be possible.
		panic(fmt.Sprintf("Failed to generate value for test doc: %v", err))
	}
	switch jsonType {
	case "string":
		*loc = fmt.Sprintf("string value %d", index)
	case "integer":
		*loc = index
	case "number":
		*loc = (1.0 / float64(index)) * 1000.0
	case "boolean":
		*loc = index%2 == 0
	case "null":
		*loc = nil
	case "object":
		*loc = make(map[string]interface{}, 0)
	case "array":
		*loc = make([]interface{}, 0)
	}
	return *loc
}
