package ops

import (
	"encoding/json"
	"time"

	pf "github.com/estuary/protocols/flow"
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
	if level > l.Level() {
		return nil
	}
	var entry = log.NewEntry(log.StandardLogger())
	entry.Time = ts
	for key, val := range fields {
		var deser interface{}
		if err := json.Unmarshal(val, &deser); err != nil {
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
