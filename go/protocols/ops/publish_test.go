package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestLogPublishing(t *testing.T) {
	var publisher = &appendPublisher{}

	PublishLog(publisher, Log_info,
		"the log message",
		"an-int", 42,
		"a-float", 3.14159,
		"a-str", "the string",
		"nested", map[string]interface{}{
			"one": 1,
			"two": 2,
		},
		"error", fmt.Errorf("failed to frobulate: %w",
			fmt.Errorf("squince doesn't look ship-shape")),
		"cancelled", context.Canceled,
	)
	PublishLog(publisher, Log_trace, "My trace level is filtered out")

	require.Equal(t, []Log{
		{
			Timestamp: publisher.logs[0].Timestamp,
			Level:     Log_info,
			Message:   "the log message",
			FieldsJsonMap: map[string]json.RawMessage{
				"a-float":   []byte("3.14159"),
				"a-str":     []byte("\"the string\""),
				"an-int":    []byte("42"),
				"cancelled": []byte("\"context canceled\""),
				"error":     []byte("\"failed to frobulate: squince doesn't look ship-shape\""),
				"nested":    []byte("{\"one\":1,\"two\":2}"),
			},
			Shard: &ShardRef{
				Name:        "task/name",
				Kind:        TaskType_capture,
				KeyBegin:    "00001111",
				RClockBegin: "00003333",
			},
			Spans: nil,
		},
	}, publisher.logs)

}

type appendPublisher struct{ logs []Log }

var _ Publisher = &appendPublisher{}

func (p *appendPublisher) PublishLog(log Log) { p.logs = append(p.logs, log) }

func (p *appendPublisher) Labels() ShardLabeling {
	return ShardLabeling{
		LogLevel: Log_debug,
		TaskName: "task/name",
		TaskType: TaskType_capture,
		Range: pf.RangeSpec{
			KeyBegin:    0x00001111,
			KeyEnd:      0x22220000,
			RClockBegin: 0x00003333,
			RClockEnd:   0x44440000,
		},
	}
}
