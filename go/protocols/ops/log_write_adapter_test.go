package ops

import (
	"encoding/json"
	"testing"

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

	var shard = &ShardRef{
		Name:        "task/name",
		Kind:        TaskType_capture,
		KeyBegin:    "00001111",
		RClockBegin: "00003333",
	}

	require.Equal(t, []Log{
		{Message: "hello world", Shard: shard, FieldsJsonMap: map[string]json.RawMessage{"stuff": []byte("42")}},
		{Message: "1", Shard: shard},
		{Message: "2", Shard: shard},
		{Message: "3", Shard: shard},
		{Message: "4", Shard: shard},
		{Message: "5", Shard: shard, FieldsJsonMap: map[string]json.RawMessage{"f1": []byte("1"), "fTwo": []byte("\"two\"")}},
	}, pub.logs)
}

type appendPublisher struct{ logs []Log }

var _ Publisher = &appendPublisher{}

func (p *appendPublisher) PublishLog(log Log)           { p.logs = append(p.logs, log) }
func (*appendPublisher) PublishStats(Stats, bool) error { panic("not called") }

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
