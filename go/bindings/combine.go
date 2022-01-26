package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/estuary/flow/go/flow/ops"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
func NewCombine(logPublisher ops.Logger) (*Combine, error) {
	var svc, err = newCombineSvc(logPublisher)
	if err != nil {
		return nil, err
	}
	var combine = &Combine{
		svc:     svc,
		drained: nil,
	}

	// Destroy the held service on garbage collection.
	runtime.SetFinalizer(combine, func(c *Combine) {
		c.Destroy()
	})
	return combine, nil
}

// Configure or re-configure the Combine.
func (c *Combine) Configure(
	fqn string,
	index *SchemaIndex,
	collection pf.Collection,
	schemaURI string,
	uuidPtr string,
	keyPtrs []string,
	fieldPtrs []string,
) error {
	combineConfigureCounter.Inc()

	c.pinnedIndex = index
	c.svc.mustSendMessage(
		uint32(pf.CombineAPI_CONFIGURE),
		&pf.CombineAPI_Config{
			SchemaIndexMemptr:  index.indexMemPtr,
			SchemaUri:          schemaURI,
			KeyPtr:             keyPtrs,
			FieldPtrs:          fieldPtrs,
			UuidPlaceholderPtr: uuidPtr,
		})

	return pollExpectNoOutput(c.svc)
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
// If Drain returns without error, the Combine may be used again. Returns statistics from the
// combiner that pertain to everything after the prior call to Drain, up through this one.
func (c *Combine) Drain(cb CombineCallback) (*pf.CombineAPI_Stats, error) {
	if c.drained == nil {
		if err := c.PrepareToDrain(); err != nil {
			return nil, err
		}
	}
	var stats pf.CombineAPI_Stats
	var err = drainCombineToCallback(c.svc, &c.drained, cb, &stats)
	if err == nil {
		recordCombineDrain(&stats)
	}
	return &stats, err
}

// Destroy the Combine service, releasing all held resources.
// Destroy may be called when it's known that a *Combine is no longer needed,
// but is optional. If not called explicitly, it will be run during garbage
// collection of the *Combine.
func (d *Combine) Destroy() {
	if d.svc != nil {
		d.svc.destroy()
		d.svc = nil
	}
}

// drainCombineToCallback drains either a Combine or a Derive, passing each document to the
// callback. The final stats will be unmarshaled into statsMessage, which will be either a
// pf.CombineAPI_Stats or a pf.DeriveAPI_Stats.
func drainCombineToCallback(
	svc *service,
	out *[]C.Out,
	cb CombineCallback,
	statsMessage proto.Unmarshaler,
) (err error) {
	// Sanity check we got triples of output frames, plus one at the end for the stats.
	if len(*out)%3 != 1 {
		panic(fmt.Sprintf("wrong number of output frames (%d; should be %% 3, plus 1)", len(*out)))
	}

	for len(*out) >= 3 {
		var doc = svc.arenaSlice((*out)[0])
		if err = cb(
			pf.CombineAPI_Code((*out)[0].code) == pf.CombineAPI_DRAINED_REDUCED_DOCUMENT,
			doc,                       // Doc.
			svc.arenaSlice((*out)[1]), // Packed key.
			svc.arenaSlice((*out)[2]), // Packed fields.
		); err != nil {
			return
		}
		*out = (*out)[3:]
	}

	// Now consume the final Stats message from the combiner.
	var statsOut = (*out)[len(*out)-1]
	var statsSlice = svc.arenaSlice(statsOut)
	if err = statsMessage.Unmarshal(statsSlice); err != nil {
		err = fmt.Errorf("unmarshaling stats: %w", err)
		return
	}

	return
}

func newCombineSvc(logPublisher ops.Logger) (*service, error) {
	return newService(
		"combine",
		func(logFilter, logDest C.int32_t) *C.Channel { return C.combine_create(logFilter, logDest) },
		func(ch *C.Channel, in C.In1) { C.combine_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.combine_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.combine_invoke16(ch, in) },
		func(ch *C.Channel) { C.combine_drop(ch) },
		logPublisher,
	)
}

/*
* These prometheus metrics track some of the same things that we get from the stats collection, but
* on a per-process basis. They are not scoped to individual shards, because that level of detail is
* already covered by the stats in the ops collections. But these metrics are useful in that they
* roll up by reactor instance, and in that they provide some level of observability that doesn't
* rely on materializing Flow collections. Basically, we shouldn't rely 100% on Flow for monitoring
* Flow.
 */

var combineConfigureCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_configure_total",
	Help: "Count of combiner configurations",
})

var combineLeftDocsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_left_docs_total",
	Help: "Count of documents input as the left hand side of combine operations",
})
var combineLeftBytesCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_left_bytes_total",
	Help: "Number of bytes input as the left hand side of combine operations",
})
var combineRightDocsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_right_docs_total",
	Help: "Count of documents input as the right hand side of combine operations",
})
var combineRightBytesCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_right_bytes_total",
	Help: "Number of bytes input as the right hand side of combine operations",
})
var combineDrainDocsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_drain_docs_total",
	Help: "Count of documents drained from combiners",
})
var combineDrainBytesCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_drain_bytes_total",
	Help: "Number of bytes drained from combiners",
})
var combineDrainOpsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "flow_combine_drain_ops_total",
	Help: "Count of number of combine operations. A single operation may combine any number of documents with any number of distinct keys.",
})

func recordCombineDrain(stats *pf.CombineAPI_Stats) {
	combineLeftDocsCounter.Add(float64(stats.Left.Docs))
	combineLeftBytesCounter.Add(float64(stats.Left.Bytes))

	combineRightDocsCounter.Add(float64(stats.Right.Docs))
	combineRightBytesCounter.Add(float64(stats.Right.Bytes))

	combineDrainDocsCounter.Add(float64(stats.Out.Docs))
	combineDrainBytesCounter.Add(float64(stats.Out.Bytes))

	combineDrainOpsCounter.Inc()
}
