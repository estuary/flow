package bindings

// #include "../../crates/bindings/flow_bindings.h"
import "C"
import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/estuary/flow/go/ops"
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
	svc     *service
	metrics combineMetrics
}

// NewCombiner builds and returns a new Combine.
func NewCombine(publisher ops.Publisher) (*Combine, error) {
	var svc, err = newCombineSvc(publisher)
	if err != nil {
		return nil, err
	}
	var combine = &Combine{
		svc: svc,
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
	collection pf.Collection,
	schemaJSON json.RawMessage,
	uuidPtr string,
	keyPtrs []string,
	fieldPtrs []string,
) error {
	combineConfigureCounter.WithLabelValues(fqn, collection.String()).Inc()
	c.metrics = newCombineMetrics(fqn, collection)

	c.svc.mustSendMessage(
		uint32(pf.CombineAPI_CONFIGURE),
		&pf.CombineAPI_Config{
			SchemaJson:         schemaJSON,
			KeyPtrs:            keyPtrs,
			FieldPtrs:          fieldPtrs,
			UuidPlaceholderPtr: uuidPtr,
		})

	return pollExpectNoOutput(c.svc)
}

// ReduceLeft reduces |doc| as a fully reduced, left-hand document.
func (c *Combine) ReduceLeft(doc json.RawMessage) error {
	c.svc.sendBytes(uint32(pf.CombineAPI_REDUCE_LEFT), doc)

	if c.svc.queuedFrames() >= 128 {
		return pollExpectNoOutput(c.svc)
	}
	return nil
}

// CombineRight combines |doc| as a partially reduced, right-hand document.
func (c *Combine) CombineRight(doc json.RawMessage) error {
	c.svc.sendBytes(uint32(pf.CombineAPI_COMBINE_RIGHT), doc)

	if c.svc.queuedFrames() >= 128 {
		return pollExpectNoOutput(c.svc)
	}
	return nil
}

// Drain combined documents, invoking the callback for each distinct group-by document
// and returning accumulated statistics of the combine operation.
// If Drain returns without error, the Combine may be used again.
func (c *Combine) Drain(cb CombineCallback) (*pf.CombineAPI_Stats, error) {
	var stats = new(pf.CombineAPI_Stats)
	var err = drainCombineToCallback(c.svc, cb, stats)

	if err == nil {
		c.metrics.recordCombineDrain(stats)
	}
	return stats, err
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
// callback. At completion accumulated statistics are unmarshalled into the provided `stats`,
// which is a *pf.CombineAPI_Stats or *pf.DeriveAPI_Stats.
func drainCombineToCallback(
	svc *service,
	cb CombineCallback,
	stats proto.Unmarshaler,
) error {

	var targetLength [4]byte
	binary.BigEndian.PutUint32(targetLength[:], 1<<22) // 4MB.

	for {
		svc.sendBytes(uint32(pf.CombineAPI_DRAIN_CHUNK), targetLength[:])
		var _, out, err = svc.poll()
		if err != nil {
			return err
		} else if len(out) == 0 {
			panic("polled DRAIN produced no output")
		}

		for len(out) != 0 {
			var code = pf.CombineAPI_Code(out[0].code)

			if code == pf.CombineAPI_DRAINED_STATS {
				if len(out) != 1 {
					panic("polled to DRAINED_STATS but unexpected `out` frames remain")
				} else if err = stats.Unmarshal(svc.arenaSlice(out[0])); err != nil {
					return fmt.Errorf("unmarshal stats: %w", err)
				}
				return nil // All documents drained.
			}

			if err = cb(
				pf.CombineAPI_Code(out[0].code) == pf.CombineAPI_DRAINED_REDUCED_DOCUMENT,
				svc.arenaSlice(out[0]), // Doc.
				svc.arenaSlice(out[1]), // Packed key.
				svc.arenaSlice(out[2]), // Packed fields.
			); err != nil {
				return err
			}
			out = out[3:]
		}
	}
}

func newCombineSvc(publisher ops.Publisher) (*service, error) {
	return newService(
		"combine",
		func(logFilter, logDest C.int32_t) *C.Channel { return C.combine_create(logFilter, logDest) },
		func(ch *C.Channel, in C.In1) { C.combine_invoke1(ch, in) },
		func(ch *C.Channel, in C.In4) { C.combine_invoke4(ch, in) },
		func(ch *C.Channel, in C.In16) { C.combine_invoke16(ch, in) },
		func(ch *C.Channel) { C.combine_drop(ch) },
		publisher,
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

func (m *combineMetrics) recordCombineDrain(stats *pf.CombineAPI_Stats) {
	m.leftDocs.Add(float64(stats.Left.Docs))
	m.leftBytes.Add(float64(stats.Left.Bytes))

	m.rightDocs.Add(float64(stats.Right.Docs))
	m.rightBytes.Add(float64(stats.Right.Bytes))

	m.drainDocs.Add(float64(stats.Out.Docs))
	m.drainBytes.Add(float64(stats.Out.Bytes))

	m.drainCounter.Inc()
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
