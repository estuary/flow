package ops

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteAdapter(t *testing.T) {
	var logs []Log
	var w = NewLogWriteAdapter(func(log Log) { logs = append(logs, log) })

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

	require.Equal(t, []Log{
		{Message: "hello world", FieldsJsonMap: map[string]json.RawMessage{"stuff": []byte("42")}},
		{Message: "1"},
		{Message: "2"},
		{Message: "3"},
		{Message: "4"},
		{Message: "5", FieldsJsonMap: map[string]json.RawMessage{"f1": []byte("1"), "fTwo": []byte("\"two\"")}},
	}, logs)
}
