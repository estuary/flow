package flow

import (
	"encoding/json"

	"github.com/estuary/protocols/fdb/tuple"
)

// Combiner combines and reduces keyed documents.
type Combiner interface {
	// ReduceLeft reduces the document on its key with a current right-hand side combined state.
	// It will be called at most once for a given key within a transaction.
	ReduceLeft(json.RawMessage) error
	// CombineRight combines the document on its key.
	CombineRight(json.RawMessage) error
	// Drain the Combiner of its documents.
	// |full| is true if this document is a full reduction (ReduceLeft was called).
	// |packedKey| is the FoundationDB tuple encoding of the document key.
	// |packedValues| are materialized fields of the materialization.
	Drain(func(full bool, doc json.RawMessage, packedKey, packedValues []byte) error) (*CombineAPI_Stats, error)
	// Destroy the Combiner.
	Destroy()
}

// MockCombiner implements Combiner by recording invocations of ReduceLeft,
// CombineRight, and Destroy, and by returning pre-arranged fixtures upon
// a call to Drain. It's a helper for testing contexts which require a Combiner.
type MockCombiner struct {
	Reduced   []json.RawMessage
	Combined  []json.RawMessage
	Destroyed bool

	drainFull   []bool
	drainKeys   [][]byte
	drainValues [][]byte
	drainDocs   []json.RawMessage
}

// ReduceLeft appends the document into Reduced.
func (c *MockCombiner) ReduceLeft(doc json.RawMessage) error {
	c.Reduced = append(c.Reduced, doc)
	return nil
}

// CombineRight appends the document into Combined.
func (c *MockCombiner) CombineRight(doc json.RawMessage) error {
	c.Combined = append(c.Combined, doc)
	return nil
}

// AddDrainFixture arranges for the document to be returned on a future call to Drain.
// The |doc| must encode to JSON without error, or AddDrainFixture panics.
func (c *MockCombiner) AddDrainFixture(full bool, doc interface{}, key, values tuple.Tuple) {
	var raw, err = json.Marshal(doc)
	if err != nil {
		panic(err)
	}

	c.drainFull = append(c.drainFull, full)
	c.drainKeys = append(c.drainKeys, key.Pack())
	c.drainValues = append(c.drainValues, values.Pack())
	c.drainDocs = append(c.drainDocs, raw)
}

// Drain invokes the callback with pre-arranged fixtures.
func (c *MockCombiner) Drain(fn func(full bool, doc json.RawMessage, packedKey, packedValues []byte) error) (*CombineAPI_Stats, error) {
	for i := range c.drainFull {
		if err := fn(c.drainFull[i], c.drainDocs[i], c.drainKeys[i], c.drainValues[i]); err != nil {
			return nil, err
		}
	}

	var dAndB = func(docs []json.RawMessage) *DocsAndBytes {
		var bytes uint64 = 0
		for _, d := range docs {
			bytes = bytes + uint64(len(d))
		}
		return &DocsAndBytes{
			Docs:  uint64(len(docs)),
			Bytes: bytes,
		}
	}

	var stats = new(CombineAPI_Stats)
	stats.Left = dAndB(c.Reduced)
	stats.Right = dAndB(c.Combined)
	stats.Out = dAndB(c.drainDocs)

	c.drainFull = nil
	c.drainKeys = nil
	c.drainValues = nil
	c.drainDocs = nil

	return stats, nil
}

// Destroy sets Destroyed to true.
func (c *MockCombiner) Destroy() {
	c.Destroyed = true
}
