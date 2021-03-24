package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"runtime"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Note that these metrics do not account for any failures that may happen in between calls to
// CombineRight and Drain or during the Drain. Any errors that occur will skew the ratio between the two, since we
// will have counted the documents that were added, but not all the documents that would have been
// drained. We _could_ increment all the counters at once after PrepareToDrain, but doing so doesn't
// seems worth the performance impact of iterating all the documents in the transaction twice.

var combineLeftDocsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_left_documents",
	Help: "Count of documents input as the left hand side of combine operations",
}, []string{"task"})
var combineLeftBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_left_bytes",
	Help: "Number of bytes input as the left hand side of combine operations",
}, []string{"task"})
var combineRightDocsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_right_documents",
	Help: "Count of documents input as the right hand side of combine operations",
}, []string{"task"})
var combineRightBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_right_bytes",
	Help: "Number of bytes input as the right hand side of combine operations",
}, []string{"task"})
var combineOutputDocsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_output_docs",
	Help: "Count of documents drained from combiners",
}, []string{"task"})
var combineOutputBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_output_bytes",
	Help: "Number of bytes drained from combiners",
}, []string{"task"})
var combineOpsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_ops",
	Help: "Count of number of combine operations. A single operation may combine any number of documents with any number of distinct keys.",
}, []string{"task"})
var combineInstanceGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "flow_combine_instances",
	Help: "Number of combiners currently active",
}, []string{"task"})

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

	// relevant metrics are stored here to avoid doing a lookup on each usage
	leftDocsCounter    prometheus.Counter
	leftBytesCounter   prometheus.Counter
	rightDocsCounter   prometheus.Counter
	rightBytesCounter  prometheus.Counter
	outputDocsCounter  prometheus.Counter
	outputBytesCounter prometheus.Counter
	opsCounter         prometheus.Counter
}

// NewCombiner builds and returns a new Combine.
func NewCombine(
	taskName string,
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

		leftDocsCounter:    combineLeftDocsCounter.WithLabelValues(taskName),
		leftBytesCounter:   combineLeftBytesCounter.WithLabelValues(taskName),
		rightDocsCounter:   combineRightDocsCounter.WithLabelValues(taskName),
		rightBytesCounter:  combineRightBytesCounter.WithLabelValues(taskName),
		outputDocsCounter:  combineOutputDocsCounter.WithLabelValues(taskName),
		outputBytesCounter: combineOutputBytesCounter.WithLabelValues(taskName),
		opsCounter:         combineOpsCounter.WithLabelValues(taskName),
	}

	var instanceGauge = combineInstanceGauge.WithLabelValues(taskName)
	// Destroy the held service on collection.
	runtime.SetFinalizer(combine, func(c *Combine) {
		instanceGauge.Dec()
		c.svc.destroy()
	})

	instanceGauge.Inc()
	return combine, nil
}

// ReduceLeft reduces |doc| as a fully reduced, left-hand document.
func (c *Combine) ReduceLeft(doc json.RawMessage) error {
	c.drained = nil // Invalidate.
	c.svc.sendBytes(uint32(pf.CombineAPI_REDUCE_LEFT), doc)
	c.leftBytesCounter.Add(float64(len(doc)))
	c.leftDocsCounter.Inc()

	if c.svc.queuedFrames() >= 128 {
		return pollExpectNoOutput(c.svc)
	}
	return nil
}

// CombineRight combines |doc| as a partially reduced, right-hand document.
func (c *Combine) CombineRight(doc json.RawMessage) error {
	c.drained = nil // Invalidate.
	c.svc.sendBytes(uint32(pf.CombineAPI_COMBINE_RIGHT), doc)

	c.rightDocsCounter.Inc()
	c.rightBytesCounter.Add(float64(len(doc)))

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
	c.opsCounter.Inc()
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
	var instrumented = instrumentCallback(c.outputDocsCounter, c.outputBytesCounter, cb)
	return drainCombineToCallback(c.svc, &c.drained, instrumented)
}

func instrumentCallback(docsCounter, bytesCounter prometheus.Counter, cb CombineCallback) CombineCallback {
	return func(full bool, doc json.RawMessage, packedKey []byte, packedFields []byte) error {
		docsCounter.Inc()
		bytesCounter.Add(float64(len(doc)))
		return cb(full, doc, packedKey, packedFields)
	}
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
