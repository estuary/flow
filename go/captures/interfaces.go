package captures

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/message"
)

// CaptureTerm represents the input to a capture implementation that's associated with a specific
// revision of the catalog Capture task.
type CaptureTerm struct {
	Revision int64
	Spec     pf.CaptureSpec
	Range    PartitionRange
	Ctx      context.Context
}

// PartitionRange is the parsed shard labels that determine the range of partitions that this shard
// will be responsible for.
type PartitionRange struct {
	// Value parsed from `estuary.dev/key-begin`
	BeginInclusive uint32
	// Value parsed from `estuary.dev/key-end`
	EndExclusive uint32
}

// ParsePartitionRange returns a PartitionRange for the shard based on its labels.
func ParsePartitionRange(shard consumer.Shard) (PartitionRange, error) {
	var labelSet = shard.Spec().LabelSet

	var doParse = func(name string) (uint32, error) {
		var value = labelSet.ValueOf(name)
		if value == "" {
			return 0, fmt.Errorf("Missing required shard label")
		}
		parsed, err := strconv.ParseUint(value, 16, 32)
		if err != nil {
			return 0, err
		}
		return uint32(parsed), nil
	}
	begin, err := doParse(labels.KeyBegin)
	if err != nil {
		return PartitionRange{}, fmt.Errorf("Parsing shard label %s: %w", labels.KeyBegin, err)
	}
	end, err := doParse(labels.KeyEnd)
	if err != nil {
		return PartitionRange{}, fmt.Errorf("Parsing shard label %s: %w", labels.KeyEnd, err)
	}
	return PartitionRange{
		BeginInclusive: begin,
		EndExclusive:   end,
	}, nil
}

func (r PartitionRange) Includes(_partitionID []byte) bool {
	// TODO: hash the id and see if the range overlaps
	return true
}

type ControlMessage struct {
	UUID      message.UUID
	Available int
	Revision  int64
}

// TODO: remove err param
func NewControlMessage(producerID message.ProducerID, available int, revision int64) *ControlMessage {
	var uuid = message.BuildUUID(producerID, message.NewClock(time.Now()), message.Flag_OUTSIDE_TXN)
	return &ControlMessage{
		UUID:      uuid,
		Available: available,
		Revision:  revision,
	}
}

var _ message.Message = (*ControlMessage)(nil) // ControlMessage implements Message

func (m *ControlMessage) GetUUID() message.UUID {
	return m.UUID
}

func (m *ControlMessage) SetUUID(_ message.UUID) {
	panic("cannot SetUUID on kinesis ControlMessage")
}

func (m *ControlMessage) NewAcknowledgement(_ pb.Journal) message.Message {
	panic("cannot NewAcknowledgement on kinesis ControlMessage")
}

type DataMessage struct {
	Document []byte
	Stream   string
	Offset   string
	Deleted  bool
}
