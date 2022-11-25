package ops

import (
	"encoding/json"

	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
)

// LocalPublisher publishes ops Logs to the local process stderr.
// Currently it uses `logrus` to do so, though this may change in the future.
type LocalPublisher struct {
	labels labels.ShardLabeling
}

var _ Publisher = &LocalPublisher{}

func NewLocalPublisher(labels labels.ShardLabeling) *LocalPublisher {
	if labels.LogLevel == pf.LogLevel_undefined {
		labels.LogLevel = logrusLogLevel()
	}
	return &LocalPublisher{labels}
}

func (p *LocalPublisher) Labels() labels.ShardLabeling { return p.labels }

func (*LocalPublisher) PublishLog(log Log) {
	var level logrus.Level
	switch log.Level {
	case pf.LogLevel_trace:
		level = logrus.TraceLevel
	case pf.LogLevel_debug:
		level = logrus.DebugLevel
	case pf.LogLevel_info:
		level = logrus.InfoLevel
	case pf.LogLevel_warn:
		level = logrus.WarnLevel
	default:
		level = logrus.ErrorLevel
	}

	var fields logrus.Fields
	if err := json.Unmarshal(log.Fields, &fields); err != nil {
		logrus.WithFields(logrus.Fields{
			"error":  err,
			"fields": string(log.Fields),
		}).Error("failed to unmarshal log fields")
	}
	logrus.StandardLogger().WithFields(fields).Log(level, log.Message)
}

/// logrusLogLevel maps the current Level of the logrus logger into a pf.LogLevel.
func logrusLogLevel() pf.LogLevel {
	switch logrus.StandardLogger().Level {
	case logrus.TraceLevel:
		return pf.LogLevel_trace
	case logrus.DebugLevel:
		return pf.LogLevel_debug
	case logrus.InfoLevel:
		return pf.LogLevel_info
	case logrus.WarnLevel:
		return pf.LogLevel_warn
	default:
		return pf.LogLevel_error
	}
}
