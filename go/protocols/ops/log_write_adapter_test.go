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

	defer func(v int) { maxLogSize = v }(maxLogSize)
	maxLogSize = 16

	w.Write([]byte(`{"message":"6"}` + "\n"))
	w.Write([]byte(`this line is way too long it just goes on and on and on`))
	w.Write([]byte(`{"message":"7"}}}}` + "\n")) // Discarded through EOL.
	w.Write([]byte(`this line is`))
	w.Write([]byte(`also way to long`))
	w.Write([]byte(`though it has`))
	w.Write([]byte(`multiple`))
	w.Write([]byte(`smaller writes`))
	w.Write([]byte(`{"message":"8"}` + "\n" + `{"message":`))
	w.Write([]byte(`"9"}` + "\n")) // Message 8 is discarded, but 9 is not.

	require.Equal(t, []Log{
		{Message: "hello world", FieldsJsonMap: map[string]json.RawMessage{"stuff": []byte("42")}},
		{Message: "1"},
		{Message: "2"},
		{Message: "3"},
		{Message: "4"},
		{Message: "5", FieldsJsonMap: map[string]json.RawMessage{"f1": []byte("1"), "fTwo": []byte("\"two\"")}},
		{Message: "6"},
		{Message: "9"},
	}, logs)
}
