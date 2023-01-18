package runtime

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/estuary/flow/go/flow"
	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
)

// TODO(johnny): We should refactor this into the `ops` package,
// along with corresponding mappings of protobuf stats into canonical
// Stats document shapes, but I'm punting on this for now.

// StatsFormatter creates stats documents for publishing into ops/<tenant>/stats collections.
// This does not actually do the publishing, since that's better handled by the runtime
// applications, which can do so transactionally. This factoring is influenced by the constraints
// imposed by materializations, which can't produce stats until after StartCommit is called.
type StatsFormatter struct {
	txnOpened       time.Time
	partitions      tuple.Tuple
	statsCollection *pf.CollectionSpec
	shard           ops.ShardRef
}

// NewStatsFormatter returns a new StatsFormatter, which will create stats documents for the given
// statsCollection. An error is returned if the statsCollection
// doesn't match the expected partitioning, since extraction of partition fields is done manually
// for ops collections.
func NewStatsFormatter(
	labeling labels.ShardLabeling,
	statsCollection *pf.CollectionSpec,
) (*StatsFormatter, error) {
	if err := ops.ValidateStatsCollection(statsCollection); err != nil {
		return nil, err
	}
	return &StatsFormatter{
		partitions:      tuple.Tuple{labeling.TaskType, labeling.TaskName},
		statsCollection: statsCollection,
		shard:           ops.NewShardRef(labeling),
	}, nil
}

// TxnOpened marks the start of a new transaction, setting the timestamp for the
// next StatsEvent.
func (s *StatsFormatter) TxnOpened() {
	s.txnOpened = time.Now().UTC()
}

// NewStatsEvent returns a new StatsEvent that's initialized with information
// about the shard and transaction timing. The transaction duration will be
// computed by subtracting the time set by `TxnOpened` from the current time.
func (s *StatsFormatter) NewStatsEvent() StatsEvent {
	return StatsEvent{
		Meta:  Meta{UUID: string(pf.DocumentUUIDPlaceholder)},
		Shard: s.shard,
		// Truncate the timestamp for stats events in order to give users a reasonable roll-up of
		// stats by default.
		Timestamp:        s.txnOpened.Truncate(time.Minute),
		TxnCount:         1,
		OpenSecondsTotal: time.Since(s.txnOpened).Seconds(),
	}
}

func (s *StatsFormatter) FormatEvent(event StatsEvent) flow.Mappable {
	var doc, err = json.Marshal(event)
	if err != nil {
		panic(fmt.Sprintf("marshaling stats json cannot fail: %v", err))
	}
	// We currently omit the key from this Mappable, which is fine because we don't actually use it
	// for publishing stats.
	return flow.Mappable{
		Spec:       s.statsCollection,
		Doc:        doc,
		Partitions: s.partitions,
	}
}

// PrepareStatsJournal returns the journal, contentType, and a new Acknowledgment message for the
// stats journal for this task. The journal is created if it does not exist. This is used in
// conjunction with Publisher.DeferPublishUncommitted, which requires these things to be provided up
// front.
func (s *StatsFormatter) PrepareStatsJournal(mapper flow.Mapper) (journal pb.Journal, contentType string, ack flow.Mappable, err error) {
	var dummy = flow.Mappable{
		Spec:       s.statsCollection,
		Partitions: s.partitions,
	}
	journal, contentType, err = mapper.Map(dummy)
	if err == nil {
		ack = flow.NewAcknowledgementMessage(s.statsCollection)
	}
	return
}

// StatsEvent is the Go struct corresponding to ops/<tenant>/stats collections. It must be
// consistent with the JSON schema: crates/build/src/ops/ops-stats-schema.json
// Many of the types within here closely resemble definitions from flow.proto,
// but we avoid re-using the proto definitions to allow this file to control the json
// representation, and to have more clarity and strictness about which fields are required.
type StatsEvent struct {
	Meta             Meta                               `json:"_meta"`
	Shard            ops.ShardRef                       `json:"shard"`
	Timestamp        time.Time                          `json:"ts"`
	TxnCount         uint64                             `json:"txnCount"`
	OpenSecondsTotal float64                            `json:"openSecondsTotal"`
	Capture          map[string]CaptureBindingStats     `json:"capture,omitempty"`
	Materialize      map[string]MaterializeBindingStats `json:"materialize,omitempty"`
	Derive           *DeriveStats                       `json:"derive,omitempty"`
}

type Meta struct {
	UUID string `json:"uuid"`
}

type DocsAndBytes struct {
	Docs  uint64 `json:"docsTotal"`
	Bytes uint64 `json:"bytesTotal"`
}

// with adds the given proto DocsAndBytes to this one and returns the result.
func (s *DocsAndBytes) with(proto *pf.DocsAndBytes) DocsAndBytes {
	return DocsAndBytes{
		Docs:  s.Docs + uint64(proto.Docs),
		Bytes: s.Bytes + uint64(proto.Bytes),
	}
}

func docsAndBytesFromProto(proto *pf.DocsAndBytes) DocsAndBytes {
	if proto == nil {
		return DocsAndBytes{}
	}
	return DocsAndBytes{
		Docs:  uint64(proto.Docs),
		Bytes: uint64(proto.Bytes),
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
	// Source collection for this transform.
	Source string       `json:"source"`
	Input  DocsAndBytes `json:"input"`
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
