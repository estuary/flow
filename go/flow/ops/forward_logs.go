package ops

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
)

// LogSourceField is the name of the field in the logs that's used to identify the original
// source of forwarded messages.
const LogSourceField = "logSource"

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
	var reader = bufio.NewScanner(logSource)
	defer logSource.Close()
	var jsonLogs, textLogs int
	// Serialize this once up front instead of separately for each message.
	var sourceDescJsonString, err = json.Marshal(sourceDesc)
	if err != nil {
		panic(fmt.Sprintf("serializing sourceDesc: %v", err))
	}
	for reader.Scan() {
		var line = reader.Bytes()
		// Remove the trailing newline, since it'd be weird for it to be included in the output
		line = bytes.Trim(line, " \n\t\r")
		if len(line) > 0 {
			// Try to parse the line as a structure json log event. If it parses, then we'll be able to
			// pass through the properties and keep everything in a nice sensible shape.
			var event = logEvent{}
			if err = json.Unmarshal(line, &event); err == nil {
				jsonLogs++
				event.Fields[LogSourceField] = json.RawMessage(sourceDescJsonString)
				// Default the timestamp and log level if they are not set.
				if event.Timestamp.IsZero() {
					event.Timestamp = time.Now().UTC()
				}
				// The zero value of a log level is PanicLevel, which means it wasn't parsed.
				// Flow doesn't use panic or fatal levels, so those were already mapped to
				// ErrorLevel during parsing.
				var level = event.Level
				if level < log.ErrorLevel {
					level = fallbackLevel
				}
				publisher.LogForwarded(event.Timestamp, level, event.Fields, event.Message)
			} else {
				// fallback to logging the raw text of each line, along with the
				textLogs++
				var fields = map[string]json.RawMessage{
					LogSourceField: json.RawMessage(sourceDescJsonString),
				}
				publisher.LogForwarded(time.Now().UTC(), fallbackLevel, fields, string(line))
			}
		}
	}
	if err = reader.Err(); err != nil {
		publisher.Log(log.ErrorLevel, log.Fields{
			"error":        err,
			LogSourceField: sourceDesc,
		}, "failed to read logs from source")
	} else {
		publisher.Log(log.TraceLevel, log.Fields{
			"jsonLines":    jsonLogs,
			"textLines":    textLogs,
			LogSourceField: sourceDesc,
		}, "finished forwarding logs")
	}
}

// parseLogLevel tries to match the given bytes to a log level string such as "info" or "DEBUG".
// Flow logs don't use "fatal" or "panic" levels, so those will be parsed as ErrorLevel.
// It returns the level and a boolean which indicates whether the parse was successful.
func parseLogLevel(b []byte) (log.Level, bool) {
	// 5 is the shortest valid length (3 for err + 2 for quotes)
	if len(b) < 5 {
		return log.PanicLevel, false
	}
	// Strip the quotes. Even if they're not quotes, we don't care, since there's no possible
	// non-string JSON token that would match any of these values.
	b = b[1 : len(b)-1]

	// Match against case-insensitive prefixes of common log levels. This is just an easy way to
	// match multiple common spellings for things like "WARN" vs "warning".
	for prefix, level := range map[string]log.Level{
		"debug": log.DebugLevel,
		"info":  log.InfoLevel,
		"trace": log.TraceLevel,
		"warn":  log.WarnLevel,
		"err":   log.ErrorLevel,
		"fatal": log.ErrorLevel,
		"panic": log.ErrorLevel,
	} {
		if len(b) >= len(prefix) && eqIgnoreAsciiCase(prefix, b[0:len(prefix)]) {
			return level, true
		}
	}

	return log.PanicLevel, false
}

// eqIgnoreAsciiCase returns true if the given inputs are the same, ignoring only ascii case.
// Ignoring ascii case is all we need for parsing log levels and field names here, so we don't
// bother with unicode case folding. This function is also called on a potentially hot path, as logs
// are forwarded, so it avoids allocating (doesn't use strings.ToLower).
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
	Level     log.Level
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
		} else if fieldMatches(k, "level", "lvl") && e.Level == log.PanicLevel {
			if lvl, ok := parseLogLevel([]byte(v)); ok {
				e.Level = lvl
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
