package ops

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// LocalPublisher publishes ops Logs to the local process stderr.
// Currently it uses `logrus` to do so, though this may change in the future.
type LocalPublisher struct {
	labels ShardLabeling
}

var _ Publisher = &LocalPublisher{}

func NewLocalPublisher(labels ShardLabeling) *LocalPublisher {
	if labels.LogLevel == Log_undefined_level {
		labels.LogLevel = logrusLogLevel()
	}
	return &LocalPublisher{labels}
}

func (p *LocalPublisher) Labels() ShardLabeling { return p.labels }

func (p *LocalPublisher) PublishLog(log Log) {
	LogToLogrus(log, p.labels.TaskName)
}

// LogToLogrus emits an ops Log to the process's standard logrus logger,
// attaching `taskName` (when non-empty) as the `task` field. The log's level
// is mapped onto the logrus level, so emission is gated by the logger's
// configured level. It's used both by LocalPublisher and to forward
// operator-observable connector logs into the data-plane's own log stream.
func LogToLogrus(log Log, taskName string) {
	var level logrus.Level
	switch log.Level {
	case Log_trace:
		level = logrus.TraceLevel
	case Log_debug:
		level = logrus.DebugLevel
	case Log_info:
		level = logrus.InfoLevel
	case Log_warn:
		level = logrus.WarnLevel
	default:
		level = logrus.ErrorLevel
	}

	var fields = make(logrus.Fields)
	var logger = logrus.StandardLogger()

	if _, ok := logger.Formatter.(*logrus.JSONFormatter); ok {
		// Logrus will JSON-encode, so pass-through our json.RawMessage fields.
		for k, v := range log.FieldsJsonMap {
			fields[k] = v
		}
	} else {
		// We're in text mode. Decode our raw JSON values.
		for k, v := range log.FieldsJsonMap {
			var vv any
			_ = json.Unmarshal(v, &vv)
			fields[k] = vv
		}
	}

	if taskName != "" && fields["task"] == nil {
		fields["task"] = taskName
	}
	logger.WithFields(fields).Log(level, log.Message)
}

// logrusLogLevel maps the current Level of the logrus logger into a pf.LogLevel.
func logrusLogLevel() Log_Level {
	switch logrus.StandardLogger().Level {
	case logrus.TraceLevel:
		return Log_trace
	case logrus.DebugLevel:
		return Log_debug
	case logrus.InfoLevel:
		return Log_info
	case logrus.WarnLevel:
		return Log_warn
	default:
		return Log_error
	}
}
