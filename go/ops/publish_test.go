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

	PublishLog(publisher, pf.LogLevel_info,
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
	PublishLog(publisher, pf.LogLevel_trace, "My trace level is filtered out")

	require.Equal(t, []Log{
		{
			Timestamp: publisher.logs[0].Timestamp,
			Level:     pf.LogLevel_info,
			Message:   "the log message",
			Fields: json.RawMessage(`{"a-float":3.14159,` +
				`"a-str":"the string",` +
				`"an-int":42,` +
				`"cancelled":"context canceled",` +
				`"error":"failed to frobulate: squince doesn't look ship-shape",` +
				`"nested":{"one":1,"two":2}}`),
			Shard: ShardRef{
				Name:        "task/name",
				Kind:        "capture",
				KeyBegin:    "00001111",
				RClockBegin: "00003333",
			},
			Spans: nil,
		},
	}, publisher.logs)

}
