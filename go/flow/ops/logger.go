package ops

import (
	"encoding/json"
	"fmt"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// Logger is an interface for publishing log messages that relate to a specific task.
// This is used so that logs can be published to Flow ops collections at runtime, but can be
// written to stderr at build/apply time.
type Logger interface {
	// Log writes a log event with the given parameters. The event may be filtered by a
	// publisher (typically based on the |level|).
	Log(level log.Level, fields log.Fields, message string) error
	// LogForwarded writes a log event that is being forwarded from some other source. The |fields|
	// passed to this function are different from the fields passed to Log. This is to allow log
	// forwarding to avoid deserializing and re-serializing all the fields of a JSON log event.
	LogForwarded(ts time.Time, level log.Level, fields map[string]json.RawMessage, message string) error
	// Level returns the current configured level filter of the Logger.
	Level() log.Level
}

func FlowToLogrusLevel(flowLevel pf.LogLevelFilter) log.Level {
	switch flowLevel {
	case pf.LogLevelFilter_TRACE:
		return log.TraceLevel
	case pf.LogLevelFilter_DEBUG:
		return log.DebugLevel
	case pf.LogLevelFilter_INFO:
		return log.InfoLevel
	case pf.LogLevelFilter_WARN:
		return log.WarnLevel
	default:
		return log.ErrorLevel
	}
}

func LogrusToFlowLevel(logrusLevel log.Level) pf.LogLevelFilter {
	switch logrusLevel {
	case log.TraceLevel:
		return pf.LogLevelFilter_TRACE
	case log.DebugLevel:
		return pf.LogLevelFilter_DEBUG
	case log.InfoLevel:
		return pf.LogLevelFilter_INFO
	case log.WarnLevel:
		return pf.LogLevelFilter_WARN
	default:
		return pf.LogLevelFilter_ERROR
	}
}

// NewLoggerWithFields wraps `delegate` and returns a new `Logger` that will add the given
// fields to each log event.
func NewLoggerWithFields(delegate Logger, add log.Fields) Logger {
	// Pre-serialize the `add` fields to their JSON forms, so that we don't have to re-do it on
	// every forwarded event.
	var addJson = make(map[string]json.RawMessage, len(add))
	for k, v := range add {
		var encoded, err = json.Marshal(v)
		if err != nil {
			panic(fmt.Sprintf("encoding of log field failed: %v, value: %v", err.Error(), v))
		}
		addJson[k] = encoded
	}
	return &withFieldsLogger{
		delegate: delegate,
		add:      add,
		addJson:  addJson,
	}
}

type withFieldsLogger struct {
	delegate Logger
	add      log.Fields
	addJson  map[string]json.RawMessage
}

func (l *withFieldsLogger) Level() log.Level {
	return l.delegate.Level()
}

func (l *withFieldsLogger) Log(level log.Level, fields log.Fields, message string) error {
	var finalFields log.Fields
	if l.requiresMapCopy(level, len(fields)) {
		finalFields = log.Fields{}
		for k, v := range l.add {
			finalFields[k] = v
		}
		for k, v := range fields {
			finalFields[k] = v
		}
	} else {
		finalFields = l.add
	}
	return l.delegate.Log(level, finalFields, message)
}

func (l *withFieldsLogger) LogForwarded(ts time.Time, level log.Level, fields map[string]json.RawMessage, message string) error {
	var finalFields map[string]json.RawMessage
	if l.requiresMapCopy(level, len(fields)) {
		finalFields = make(map[string]json.RawMessage, len(fields)+len(l.addJson))
		for k, v := range l.addJson {
			finalFields[k] = v
		}
		for k, v := range fields {
			finalFields[k] = v
		}
	} else {
		finalFields = l.addJson
	}
	return l.delegate.LogForwarded(ts, level, finalFields, message)
}

// requiresMapCopy returns true if the logger needs to copy the fields map in order to combine the
// fields passed to `Log` or `LogForwarded` with the additional fields. The point is to avoid
// copying the map if no additional fields were given, or if we're not going to log this event
// anyway due to the verbosity.
func (l *withFieldsLogger) requiresMapCopy(level log.Level, givenFieldsLen int) bool {
	return givenFieldsLen > 0 && level <= l.delegate.Level()
}

type stdLogAppender struct{}

// Level implements ops.Logger for stdLogAppender
func (stdLogAppender) Level() log.Level {
	return log.GetLevel()
}

// Log implements ops.Logger for stdLogAppender
func (l stdLogAppender) Log(level log.Level, fields log.Fields, message string) error {
	if level > l.Level() {
		return nil
	}
	log.WithFields(fields).Log(level, message)
	return nil
}

// LogForwarded implements ops.Logger for stdLogAppender
func (l stdLogAppender) LogForwarded(ts time.Time, level log.Level, fields map[string]json.RawMessage, message string) error {
	var entry = log.NewEntry(log.StandardLogger())
	entry.Time = ts
	for key, val := range fields {
		var deser interface{}
		if err := json.Unmarshal(val, &deser); err == nil {
			entry.Data[key] = deser
		}
	}
	entry.Log(level, message)
	return nil
}

// StdLogger returns a Logger that just forwards to the logrus package. This is used
// during operations that happen outside of the Flow runtime (such as flowctl build or apply).
func StdLogger() Logger {
	return stdLogAppender{}
}
