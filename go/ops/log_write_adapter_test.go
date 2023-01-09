package ops

import (
	"encoding/json"
	"testing"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestWriteAdapter(t *testing.T) {
	var pub = &appendPublisher{}
	var w = NewLogWriteAdapter(pub)

	// Multiple writes per line.
	w.Write([]byte(`{"message"`))
	w.Write([]byte(`:"hello world","fields":{"stuff": 42 }}` + "\n"))

	// Multiple lines per write.
	w.Write([]byte(`{"message":"1"}` + "\n invalid json! \n" + `{"message":"2"}` + "\n" + `{"message":`))
	w.Write([]byte(`"3"}` + "\n"))

	// Exact lines per write.
	w.Write([]byte(`{"message":"4"}` + "\n"))
	w.Write([]byte(`more invalid json!` + "\n"))
	w.Write([]byte(`{"message":"5", "fields":{"f1":1, "fTwo":"two"}}` + "\n"))

	var shard = ShardRef{
		Name:        "task/name",
		Kind:        "capture",
		KeyBegin:    "00001111",
		RClockBegin: "00003333",
	}

	require.Equal(t, []Log{
		{Message: "hello world", Shard: shard, Fields: json.RawMessage(`{"stuff": 42 }`)},
		{Message: "1", Shard: shard, Fields: json.RawMessage("{}")},
		{Message: "2", Shard: shard, Fields: json.RawMessage("{}")},
		{Message: "3", Shard: shard, Fields: json.RawMessage("{}")},
		{Message: "4", Shard: shard, Fields: json.RawMessage("{}")},
		{Message: "5", Shard: shard, Fields: json.RawMessage(`{"f1":1, "fTwo":"two"}`)},
	}, pub.logs)
}

type appendPublisher struct{ logs []Log }

// PublishStats implements Publisher
func (*appendPublisher) PublishStats(StatsEvent) {
	// no-op
}

var _ Publisher = &appendPublisher{}

func (p *appendPublisher) PublishLog(log Log) { p.logs = append(p.logs, log) }
func (p *appendPublisher) Labels() labels.ShardLabeling {
	return labels.ShardLabeling{
		LogLevel: pf.LogLevel_debug,
		TaskName: "task/name",
		TaskType: labels.TaskTypeCapture,
		Range: pf.RangeSpec{
			KeyBegin:    0x00001111,
			KeyEnd:      0x22220000,
			RClockBegin: 0x00003333,
			RClockEnd:   0x44440000,
		},
	}
}
