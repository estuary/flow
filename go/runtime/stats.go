package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
)

// StatsFormatter creates stats documents for publishing into ops/<tenant>/stats collections.
// This does not actually do the publishing, since that's better handled by the runtime
// applications, which can do so transactionally. This factoring is influenced by the constraints
// imposed by materializations, which can't produce stats until after StartCommit is called.
type StatsFormatter struct {
	partitions      tuple.Tuple
	statsCollection *pf.CollectionSpec
	shard           *ShardRef
	task            pf.Task
}

// NewStatsFormatter returns a new StatsFormatter, which will create stats documents for the given
// statsCollection, pertaining to the given task. An error is returned if the statsCollection
// doesn't match the expected partitioning, since extraction of partition fields is done manually
// for ops collections.
func NewStatsFormatter(
	labeling labels.ShardLabeling,
	statsCollection *pf.CollectionSpec,
	task pf.Task,
) (*StatsFormatter, error) {
	if err := validateOpsCollection(statsCollection); err != nil {
		return nil, err
	}
	var shard, partitions = shardAndPartitions(labeling)
	return &StatsFormatter{
		partitions:      partitions,
		statsCollection: statsCollection,
		shard:           &shard,
		task:            task,
	}, nil
}

// CaptureTxnStats returns a new flow.Mappable with statistics for a single capture transaction that
// was begun at `txnOpened`. The `statsPerBinding` may contain nil values for bindings that did not
// participate in the transaction, in which case they will not be included in the resulting stats
// document.
func (p *StatsFormatter) CaptureTxnStats(txnOpened time.Time, statsPerBinding []*pf.CombineAPI_Stats) flow.Mappable {
	var event = p.captureStats(txnOpened, statsPerBinding)
	return p.newDocument(event)
}

// DeriveTxnStats returns a new flow.Mappable with statistics for a single derive transaction that
// was begun at `txnOpened`. The derive stats `Transforms` may contain nil values for transforms
// that did not participate in the transaction, in which case they will not be included in the
// resulting stats document.
func (p *StatsFormatter) DeriveTxnStats(txnOpened time.Time, deriveStats *pf.DeriveAPI_Stats) flow.Mappable {
	var event = p.deriveStats(txnOpened, deriveStats)
	return p.newDocument(event)
}

// MaterializeTxnStats returns a new flow.Mappable with statistics for a single materialize transaction that
// was begun at `txnOpened`. The `statsPerBinding` may contain nil values for bindings that did not
// participate in the transaction, in which case they will not be included in the resulting stats
// document.
func (p *StatsFormatter) MaterializeTxnStats(txnOpened time.Time, statsPerBinding []*pf.CombineAPI_Stats) flow.Mappable {
	var event = p.materializationStats(txnOpened, statsPerBinding)
	return p.newDocument(event)
}

func (p *StatsFormatter) newDocument(event StatsEvent) flow.Mappable {
	var doc, err = json.Marshal(event)
	if err != nil {
		panic(fmt.Sprintf("marshaling stats json cannot fail: %v", err))
	}
	// We currently omit the key from this Mappable, which is fine because we don't actually use it
	// for publishing stats.
	return flow.Mappable{
		Spec:       p.statsCollection,
		Doc:        doc,
		Partitions: p.partitions,
	}
}

func (s *StatsFormatter) materializationStats(txnOpened time.Time, statsPerBinding []*pf.CombineAPI_Stats) StatsEvent {
	var spec = s.task.(*pf.MaterializationSpec)
	var stats = make(map[string]MaterializeBindingStats)
	for i, bindingStats := range statsPerBinding {
		if bindingStats != nil { // Skip bindings that didn't participate
			var name = spec.Bindings[i].Collection.Collection.String()
			// It's possible for multiple bindings to use the same collection, in which case the
			// stats should be summed.
			var prevStats = stats[name]
			stats[name] = MaterializeBindingStats{
				Left:  prevStats.Left.with(bindingStats.Left),
				Right: prevStats.Right.with(bindingStats.Right),
				Out:   prevStats.Out.with(bindingStats.Out),
			}
		}
	}
	var event = s.newEvent(txnOpened)
	event.Materialize = stats
	return event
}

func (s *StatsFormatter) captureStats(txnOpened time.Time, statsPerBinding []*pf.CombineAPI_Stats) StatsEvent {
	var captureSpec = s.task.(*pf.CaptureSpec)
	var captureStats = make(map[string]CaptureBindingStats)
	for i, bindingStats := range statsPerBinding {
		if bindingStats != nil { // Skip bindings that didn't participate
			var name = captureSpec.Bindings[i].Collection.Collection.String()
			// It's possible for multiple bindings to use the same collection, in which case the
			// stats should be summed.
			var prevStats = captureStats[name]
			captureStats[name] = CaptureBindingStats{
				Right: prevStats.Right.with(bindingStats.Right),
				Out:   prevStats.Out.with(bindingStats.Out),
			}
		}
	}
	var event = s.newEvent(txnOpened)
	event.Capture = captureStats
	return event
}

func (s *StatsFormatter) deriveStats(txnOpened time.Time, txnStats *pf.DeriveAPI_Stats) StatsEvent {
	// assert that our task is a derivation and panic if not.
	var derivationSpec = s.task.(*pf.DerivationSpec)
	var tfStats = make(map[string]DeriveTransformStats, len(txnStats.Transforms))
	// Only output register stats if at least one participating transform has an update lambda. This
	// allows for distinguishing between transforms where no update was invoked (Register stats will
	// be omitted) and transforms where the update lambda happened to only update existing registers
	// (Created will be 0).
	var includesUpdate = false
	for i, tf := range txnStats.Transforms {
		// Don't include transforms that didn't participate in this transaction.
		if tf != nil && tf.Input != nil {
			var tfSpec = derivationSpec.Transforms[i]
			var stats = DeriveTransformStats{
				Input: docsAndBytesFromProto(tf.Input),
			}
			if tfSpec.UpdateLambda != nil {
				includesUpdate = true
				stats.Update = &InvokeStats{
					Out:          docsAndBytesFromProto(tf.Update.Output),
					SecondsTotal: tf.Update.TotalSeconds,
				}
			}
			if tfSpec.PublishLambda != nil {
				stats.Publish = &InvokeStats{
					Out:          docsAndBytesFromProto(tf.Publish.Output),
					SecondsTotal: tf.Publish.TotalSeconds,
				}
			}
			tfStats[tfSpec.Transform.String()] = stats
		}
	}
	var event = s.newEvent(txnOpened)
	event.Derive = &DeriveStats{
		Transforms: tfStats,
		Out:        docsAndBytesFromProto(txnStats.Output),
	}
	if includesUpdate {
		event.Derive.Registers = &DeriveRegisterStats{
			CreatedTotal: txnStats.Registers.Created,
		}
	}
	return event
}

func (s *StatsFormatter) newEvent(txnOpened time.Time) StatsEvent {
	return StatsEvent{
		Meta:  Meta{UUID: string(pf.DocumentUUIDPlaceholder)},
		Shard: s.shard,
		// Truncate the timestamp for stats events in order to give users a reasonable roll-up of
		// stats by default.
		Timestamp:        txnOpened.Truncate(time.Minute),
		TxnCount:         1,
		OpenSecondsTotal: time.Since(txnOpened).Seconds(),
	}
}

// statsCollection returns the collection to which stats for the given task name are written.
func statsCollection(taskName string) pf.Collection {
	return pf.Collection(fmt.Sprintf("ops/%s/stats", strings.Split(taskName, "/")[0]))
}

// StatsEvent is the Go struct corresponding to ops/<tenant>/stats collections. It must be
// consistent with the JSON schema: crates/build/src/ops/ops-stats-schema.json
// Many of the types within here closely resemble definitions from flow.proto,
// but we avoid re-using the proto definitions to allow this file to control the json
// representation, and to have more clarity and strictness about which fields are required.
type StatsEvent struct {
	Meta             Meta                               `json:"_meta"`
	Shard            *ShardRef                          `json:"shard"`
	Timestamp        time.Time                          `json:"ts"`
	TxnCount         uint64                             `json:"txnCount"`
	OpenSecondsTotal float64                            `json:"openSecondsTotal"`
	Capture          map[string]CaptureBindingStats     `json:"capture,omitempty"`
	Materialize      map[string]MaterializeBindingStats `json:"materialize,omitempty"`
	Derive           *DeriveStats                       `json:"derive,omitempty"`
}

type DocsAndBytes struct {
	Docs  uint64 `json:"docsTotal"`
	Bytes uint64 `json:"bytesTotal"`
}

// with adds the given proto DocsAndBytes to this one and returns the result.
func (s *DocsAndBytes) with(proto *pf.DocsAndBytes) DocsAndBytes {
	return DocsAndBytes{
		Docs:  s.Docs + proto.Docs,
		Bytes: s.Bytes + proto.Bytes,
	}
}

func docsAndBytesFromProto(proto *pf.DocsAndBytes) DocsAndBytes {
	if proto == nil {
		return DocsAndBytes{}
	}
	return DocsAndBytes{
		Docs:  proto.Docs,
		Bytes: proto.Bytes,
	}
}

type CaptureBindingStats struct {
	Right DocsAndBytes `json:"right"`
	Out   DocsAndBytes `json:"out"`
}

type MaterializeBindingStats struct {
	Left  DocsAndBytes `json:"left"`
	Right DocsAndBytes `json:"right"`
	Out   DocsAndBytes `json:"out"`
}

type InvokeStats struct {
	Out          DocsAndBytes `json:"out"`
	SecondsTotal float64      `json:"secondsTotal"`
}

type DeriveTransformStats struct {
	Input DocsAndBytes `json:"input"`
	// At least one of Update or Publish must be present in the output,
	// but either one can be optional. This is to avoid outputting zeroed out invocation stats for
	// lambdas that the user hasn't defined.
	Update  *InvokeStats `json:"update,omitempty"`
	Publish *InvokeStats `json:"publish,omitempty"`
}

type DeriveRegisterStats struct {
	CreatedTotal uint64 `json:"createdTotal"`
}

type DeriveStats struct {
	Transforms map[string]DeriveTransformStats `json:"transforms"`
	Out        DocsAndBytes                    `json:"out"`
	Registers  *DeriveRegisterStats            `json:"registers,omitempty"`
}
