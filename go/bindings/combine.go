package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/json"
	"fmt"
	"runtime"

	pf "github.com/estuary/protocols/flow"
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
	stats       combineStats
	metrics     combineMetrics
}

// NewCombiner builds and returns a new Combine.
func NewCombine() *Combine {
	var combine = &Combine{
		svc:     newCombineSvc(),
		drained: nil,
		stats:   combineStats{},
		metrics: combineMetrics{},
	}

	// Destroy the held service on garbage collection.
	runtime.SetFinalizer(combine, func(c *Combine) {
		c.Destroy()
	})
	return combine
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
	combineConfigureCounter.WithLabelValues(fqn, collection.String()).Inc()

	c.metrics = newCombineMetrics(fqn, collection)
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
	c.stats.leftDocs++
	c.stats.leftBytes += len(doc)

	if c.svc.queuedFrames() >= 128 {
		return pollExpectNoOutput(c.svc)
	}
	return nil
}

// CombineRight combines |doc| as a partially reduced, right-hand document.
func (c *Combine) CombineRight(doc json.RawMessage) error {
	c.drained = nil // Invalidate.
	c.svc.sendBytes(uint32(pf.CombineAPI_COMBINE_RIGHT), doc)

	c.stats.rightDocs++
	c.stats.rightBytes += len(doc)

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
func (c *Combine) Drain(cb CombineCallback) (err error) {
	defer c.stats.reset()
	if c.drained == nil {
		if err = c.PrepareToDrain(); err != nil {
			return
		}
	}
	c.stats.drainDocs, c.stats.drainBytes, err = drainCombineToCallback(c.svc, &c.drained, cb)
	if err == nil {
		c.metrics.recordDrain(&c.stats)
	}
	return
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

func drainCombineToCallback(
	svc *service,
	out *[]C.Out,
	cb CombineCallback,
) (nDocs, nBytes int, err error) {
	// Sanity check we got triples of output frames.
	if len(*out)%3 != 0 {
		panic(fmt.Sprintf("wrong number of output frames (%d; should be %% 3)", len(*out)))
	}

	for len(*out) >= 3 {
		var doc = svc.arenaSlice((*out)[0])
		nDocs++
		nBytes += len(doc)
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

	return
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

var combineConfigureCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_configure_total",
	Help: "Count of combiner configurations",
}, []string{"shard", "collection"})

var combineLeftDocsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_left_docs_total",
	Help: "Count of documents input as the left hand side of combine operations",
}, []string{"shard", "collection"})
var combineLeftBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_left_bytes_total",
	Help: "Number of bytes input as the left hand side of combine operations",
}, []string{"shard", "collection"})
var combineRightDocsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_right_docs_total",
	Help: "Count of documents input as the right hand side of combine operations",
}, []string{"shard", "collection"})
var combineRightBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_right_bytes_total",
	Help: "Number of bytes input as the right hand side of combine operations",
}, []string{"shard", "collection"})
var combineDrainDocsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_drain_docs_total",
	Help: "Count of documents drained from combiners",
}, []string{"shard", "collection"})
var combineDrainBytesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_drain_bytes_total",
	Help: "Number of bytes drained from combiners",
}, []string{"shard", "collection"})
var combineOpsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "flow_combine_drain_ops_total",
	Help: "Count of number of combine operations. A single operation may combine any number of documents with any number of distinct keys.",
}, []string{"shard", "collection"})

type combineStats struct {
	leftDocs   int
	leftBytes  int
	rightDocs  int
	rightBytes int
	drainDocs  int
	drainBytes int
}

func (s *combineStats) reset() {
	*s = combineStats{}
}

type combineMetrics struct {
	leftDocs  prometheus.Counter
	leftBytes prometheus.Counter

	rightDocs  prometheus.Counter
	rightBytes prometheus.Counter

	drainDocs  prometheus.Counter
	drainBytes prometheus.Counter

	drainCounter prometheus.Counter
}

func newCombineMetrics(fqn string, collection pf.Collection) combineMetrics {
	var name = collection.String()

	return combineMetrics{
		leftDocs:  combineLeftDocsCounter.WithLabelValues(fqn, name),
		leftBytes: combineLeftBytesCounter.WithLabelValues(fqn, name),

		rightDocs:  combineRightDocsCounter.WithLabelValues(fqn, name),
		rightBytes: combineRightBytesCounter.WithLabelValues(fqn, name),

		drainDocs:  combineDrainDocsCounter.WithLabelValues(fqn, name),
		drainBytes: combineDrainBytesCounter.WithLabelValues(fqn, name),

		drainCounter: combineOpsCounter.WithLabelValues(fqn, name),
	}
}

func (m *combineMetrics) recordDrain(stats *combineStats) {
	m.leftDocs.Add(float64(stats.leftDocs))
	m.leftBytes.Add(float64(stats.leftBytes))

	m.rightDocs.Add(float64(stats.rightDocs))
	m.rightBytes.Add(float64(stats.rightBytes))

	m.drainDocs.Add(float64(stats.drainDocs))
	m.drainBytes.Add(float64(stats.drainBytes))

	m.drainCounter.Inc()
}
