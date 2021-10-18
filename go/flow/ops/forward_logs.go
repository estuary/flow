package ops

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
)

const LOG_SOURCE_FIELD = "logSource"

// ForwardLogs reads lines from |logSource| and forwards them to the |publisher|. It attempts to
// parse each line as a JSON-encoded structured log event, so that it will be logged at the level
// indicated in the line. If it's unable to parse the line, then the whole line will be used as the
// message of the log event, and it will be logged at the |fallbackLevel|. The |sourceDesc| will be
// added as the "logSource" field on every event, regardless of whether it parses. The |logSource|
// will be closed automatically after the first error or EOF is encountered.
//
// The parsing of JSON log lines is intentionally pretty permissive. The parsing will attempt to
// extract fields for the level, timestamp, and message by looking for properties matching those
// names, ignoring (ascii) case. Common abreviations of those fields (such as "ts" for "timestamp")
// are also accepted. If your log event defines multiple properties that match a given field (e.g.
// both "message" and "msg"), then which one gets used is undefined. All other fields present in the
// JSON object, apart from those that defined the level, timestamp, or message, will be added to the
// `fields` of the event.
// For an example of how to configure a `tracing_subscriber` in Rust so that
// it's compatible with this format, check out: crates/bindings/src/logging.rs
func ForwardLogs(sourceDesc string, fallbackLevel log.Level, logSource io.ReadCloser, publisher LogPublisher) {
	var reader = bufio.NewReader(logSource)
	defer logSource.Close()
	var jsonLogs, textLogs int
	// Serialize this once up front instead of separately for each message.
	var sourceDescJsonString, err = json.Marshal(sourceDesc)
	if err != nil {
		panic(fmt.Sprintf("serializing sourceDesc: %v", err))
	}
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				publisher.Log(log.ErrorLevel, log.Fields{
					"error":          err,
					LOG_SOURCE_FIELD: sourceDesc,
				}, "failed to read logs from source")
			}
			break
		}
		// Remove the trailing newline, since it'd be weird for it to be included in the output
		line = bytes.TrimSuffix(line, []byte{'\n'})
		if len(line) == 0 {
			continue
		}

		// Try to parse the line as a structure json log event. If it parses, then we'll be able to
		// pass through the properties and keep everything in a nice sensible shape.
		var event = logEvent{}
		if err = json.Unmarshal(line, &event); err == nil {
			jsonLogs++
			event.Fields[LOG_SOURCE_FIELD] = json.RawMessage(sourceDescJsonString)
			// Default the timestamp and log level if they are not set.
			if event.Timestamp.IsZero() {
				event.Timestamp = time.Now().UTC()
			}
			var level = fallbackLevel
			if !event.Level.isZero() {
				level = log.Level(event.Level)
			}
			publisher.LogForwarded(event.Timestamp, level, event.Fields, event.Message)
		} else {
			// fallback to logging the raw text of each line, along with the
			textLogs++
			var fields = map[string]json.RawMessage{
				LOG_SOURCE_FIELD: json.RawMessage(sourceDescJsonString),
			}
			publisher.LogForwarded(time.Now().UTC(), fallbackLevel, fields, string(line))
		}
	}
	publisher.Log(log.TraceLevel, log.Fields{
		"jsonLines":      jsonLogs,
		"textLines":      textLogs,
		LOG_SOURCE_FIELD: sourceDesc,
	}, "finished forwarding logs")
}

// jsonLogLevel is just a wrapper around a log.Level that allows for more flexible deserialization.
type jsonLogLevel log.Level

func (l jsonLogLevel) isZero() bool {
	return l == 0
}

var INVALID_LOG_LEVEL = errors.New("invalid log level")

func (l *jsonLogLevel) UnmarshalJSON(b []byte) error {
	// 5 is the shortest valid length (3 for err + 2 for quotes)
	if len(b) < 5 {
		return INVALID_LOG_LEVEL
	}
	// Strip the quotes. Even if they're not quotes, we don't care, since there's no possible
	// non-string JSON token that would match any of these values.
	b = b[1 : len(b)-1]

	// Match against case-insensitive prefixes of common log levels. This is just an easy way to
	// match multiple common spellings for things like "WARN" vs "warning".
	for _, candidate := range []struct {
		prefix string
		level  log.Level
	}{
		{
			prefix: "debug",
			level:  log.DebugLevel,
		},
		{
			prefix: "info",
			level:  log.InfoLevel,
		},
		{
			prefix: "trace",
			level:  log.TraceLevel,
		},
		{
			prefix: "warn",
			level:  log.WarnLevel,
		},
		{
			prefix: "err",
			level:  log.ErrorLevel,
		},
		{
			prefix: "fatal",
			level:  log.ErrorLevel,
		},
		{
			prefix: "panic",
			level:  log.ErrorLevel,
		},
	} {
		if len(b) >= len(candidate.prefix) && eqIgnoreAsciiCase(candidate.prefix, b[0:len(candidate.prefix)]) {
			*l = jsonLogLevel(candidate.level)
			return nil
		}
	}

	return INVALID_LOG_LEVEL
}

func eqIgnoreAsciiCase(a string, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, aByte := range []byte(a) {
		if aByte != b[i] && (aByte^32) != b[i] {
			return false
		}
	}
	return true
}

type logEvent struct {
	Level     jsonLogLevel
	Timestamp time.Time
	Fields    map[string]json.RawMessage
	Message   string
}

func (e *logEvent) UnmarshalJSON(b []byte) error {
	*e = logEvent{}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	for k, v := range m {
		if fieldMatches(k, "timestamp", "time", "ts") && e.Timestamp.IsZero() {
			var t time.Time
			if err := json.Unmarshal([]byte(v), &t); err == nil {
				e.Timestamp = t
				delete(m, k)
			}
		} else if fieldMatches(k, "level", "lvl") && e.Level.isZero() {
			if err := json.Unmarshal([]byte(v), &e.Level); err == nil {
				delete(m, k)
			}
		} else if fieldMatches(k, "message", "msg") && e.Message == "" {
			var s string
			if err := json.Unmarshal(v, &s); err == nil {
				e.Message = s
				delete(m, k)
			}
		}
	}
	e.Fields = m
	return nil
}

func fieldMatches(field string, allowed ...string) bool {
	for _, candidate := range allowed {
		if eqIgnoreAsciiCase(field, []byte(candidate)) {
			return true
		}
	}
	return false
}
