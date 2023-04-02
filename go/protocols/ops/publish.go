package ops

import (
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/types"
)

// Publisher of operation Logs and Stats.
type Publisher interface {
	// PublishLog publishes a Log instance.
	PublishLog(Log)
	// PublishStats publishes a StatsEvent.
	PublishStats(stats Stats, immediate bool) error
	// Level to log at.
	Labels() ShardLabeling
}

// PublishLog constructs and publishes a Log using the given Publisher.
// Fields must be pairs of a string key followed by a JSON-encodable interface{} value.
// PublishLog panics if `fields` are odd, or if a field isn't a string,
// or if it cannot be encoded as JSON.
func PublishLog(publisher Publisher, level Log_Level, message string, fields ...interface{}) {
	var labels = publisher.Labels()
	if labels.LogLevel < level {
		return
	}

	// NOTE(johnny): We panic because incorrect fields are a developer
	// implementation error, and not a user or input error.
	if len(fields)%2 != 0 {
		panic(fmt.Sprintf("fields must be of even length: %#v", fields))
	}

	var fieldsMap = make(map[string]json.RawMessage, len(fields)/2)
	for i := 0; i != len(fields); i += 2 {
		var key = fields[i].(string)
		var value = fields[i+1]

		// Errors typically have JSON struct marshalling behavior and appear as '{}',
		// so explicitly cast them to their displayed string.
		if err, ok := value.(error); ok {
			value = err.Error()
		}

		var valueRaw, err = json.Marshal(value)
		if err != nil {
			panic(err)
		}
		fieldsMap[key] = valueRaw
	}

	publisher.PublishLog(Log{
		Shard:         NewShardRef(publisher.Labels()),
		Timestamp:     types.TimestampNow(),
		Level:         level,
		Message:       message,
		FieldsJsonMap: fieldsMap,
		Spans:         nil, // Not supported from Go.
	})
}
