package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	po "github.com/estuary/flow/go/protocols/ops"
	"github.com/stretchr/testify/require"
)

func TestLogPublishing(t *testing.T) {
	var publisher = &appendPublisher{}

	PublishLog(publisher, po.Log_info,
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
	PublishLog(publisher, po.Log_trace, "My trace level is filtered out")

	require.Equal(t, []Log{
		{
			Timestamp: publisher.logs[0].Timestamp,
			Level:     po.Log_info,
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
				Kind:        po.Shard_capture,
				KeyBegin:    "00001111",
				RClockBegin: "00003333",
			},
			Spans: nil,
		},
	}, publisher.logs)

}
