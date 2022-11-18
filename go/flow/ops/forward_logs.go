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
// message of the log event, and it will be logged at the |WarnLevel|. The |sourceDesc| will be
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
func ForwardLogs(sourceDesc string, logSource io.ReadCloser, publisher Logger) {
	var reader = bufio.NewScanner(logSource)
	defer logSource.Close()
	var forwarder = newLogForwarder(sourceDesc, publisher)
	for reader.Scan() {
		forwarder.forwardLine(reader.Bytes())
	}

	_ = forwarder.logFinished(reader.Err())
}

// NewLogForwardWriter returns a new `io.WriteCloser` that forwards all data written to it as logs
// to the given publisher. The data written will be treated in exactly the same way as it is for
// `ForwardLogs`.
func NewLogForwardWriter(sourceDesc string, publisher Logger) *LogForwardWriter {
	return &LogForwardWriter{
		logForwarder: newLogForwarder(sourceDesc, publisher),
	}
}

// LogForwardWriter is an `io.WriteCloser` that forwards all the bytes written to it to an
// `ops.LogPublisher`. It is designed to be used as the stderr of child processes so that logs from
// the process will be forwarded.
type LogForwardWriter struct {
	logForwarder
	buffer []byte
}

// maxLogLine is the maximum allowable length of any single log line that we will try to parse.
// If logging output contains a sequence longer than this without a newline character, then it will
// be broken up into chunks of this size, which are then processed as normal. The actual value here
// was chosen somewhat arbitrarily.
const maxLogLine = 65536

// Write implements `io.Writer`
func (f *LogForwardWriter) Write(p []byte) (n int, err error) {

	var toWrite = p
	var newlineIndex = bytes.IndexByte(toWrite, '\n')
	for newlineIndex >= 0 {
		var line = toWrite[:newlineIndex]
		if len(f.buffer) > 0 {
			line = append(f.buffer, line...)
		}
		if len(line) > 0 {
			if err := f.forwardLine(line); err != nil {
				return 0, fmt.Errorf("forwarding logs: %w", err)
			}
		}
		f.buffer = f.buffer[:0]
		toWrite = toWrite[newlineIndex+1:]
		newlineIndex = bytes.IndexByte(toWrite, '\n')
	}
	// Ensure that the buffer doesn't grow indefinitely if the data does not include newlines.
	// In this case, we'll just split the input at an arbitrary maximum length.
	for len(f.buffer)+len(toWrite) >= maxLogLine {
		// Append enough data to the buffer from toWrite to get to the max
		var add = maxLogLine - len(f.buffer)
		if len(toWrite) < add {
			add = len(toWrite)
		}
		f.buffer = append(f.buffer, toWrite[:add]...)
		toWrite = toWrite[add:]
		if err := f.forwardLine(f.buffer); err != nil {
			return 0, fmt.Errorf("forwarding logs: %w", err)
		}
		f.buffer = f.buffer[:0]
	}
	// No newline? No problem. Just buffer the data until a newline is encountered.
	if len(toWrite) > 0 {
		f.buffer = append(f.buffer, toWrite...)
	}
	return len(p), nil
}

// Close implements `io.Closer`
func (f *LogForwardWriter) Close() (err error) {
	// Is there some buffered data? If so, then we may emit one last log event
	// since we now know that a newline is not forthcoming.
	if len(f.buffer) > 0 {
		err = f.forwardLine(f.buffer)
		f.buffer = nil
	}
	return f.logFinished(err)
}

// logForwarder is an internal implementation for log forwarding, which is used by both `ForwardLogs`
// and by `LogForwardWriter`.
type logForwarder struct {
	// Running counters of the number of lines processed for each type.
	jsonLines int
	textLines int
	// Added as the `LogSourceField` to each event.
	sourceDesc string
	// Pre-serialized source desc, so we can avoid allocating on every event.
	sourceDescJsonString json.RawMessage
	publisher            Logger
}

func newLogForwarder(sourceDesc string, publisher Logger) logForwarder {
	var sourceDescJsonString, err = json.Marshal(sourceDesc)
	if err != nil {
		panic(fmt.Sprintf("serializing sourceDesc: %v", err))
	}
	return logForwarder{
		sourceDesc:           sourceDesc,
		sourceDescJsonString: json.RawMessage(sourceDescJsonString),
		publisher:            publisher,
	}
}

func (f *logForwarder) forwardLine(line []byte) error {
	// Trim trailing space, but not preceeding space, since indentation might actually be
	// significant or helpful.
	line = bytes.TrimRight(line, " \n\t\r")
	if len(line) == 0 {
		return nil
	}
	// Try to parse the line as a structure json log event. If it parses, then we'll be able to
	// pass through the properties and keep everything in a nice sensible shape.
	var event = logEvent{}
	if err := json.Unmarshal(line, &event); err == nil {
		f.jsonLines++
		event.Fields[LogSourceField] = f.sourceDescJsonString
		// Default the timestamp and log level if they are not set.
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now().UTC()
		}
		var level = log.WarnLevel
		if event.Level >= log.ErrorLevel {
			level = event.Level
		}
		return f.publisher.LogForwarded(event.Timestamp, level, event.Fields, event.Message)
	} else {
		// Logging the raw text of each line, along with the
		f.textLines++
		var fields = map[string]json.RawMessage{
			LogSourceField: f.sourceDescJsonString,
		}
		return f.publisher.LogForwarded(time.Now().UTC(), log.WarnLevel, fields, string(line))
	}
}

func (f *logForwarder) logFinished(err error) error {
	if err != nil {
		f.publisher.Log(log.ErrorLevel, log.Fields{
			"error":        err,
			LogSourceField: f.sourceDesc,
		}, "failed to read logs from source")
		return err
	} else {
		return f.publisher.Log(log.TraceLevel, log.Fields{
			"jsonLines":    f.jsonLines,
			"textLines":    f.textLines,
			LogSourceField: f.sourceDesc,
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
	// Fields are kept as raw messages to avoid unnecessary parsing.
	Fields  map[string]json.RawMessage
	Message string
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
