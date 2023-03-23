package ops

import (
	"encoding/json"

	"github.com/estuary/flow/go/labels"
	po "github.com/estuary/flow/go/protocols/ops"
	"github.com/sirupsen/logrus"
)

// LocalPublisher publishes ops Logs to the local process stderr.
// Currently it uses `logrus` to do so, though this may change in the future.
type LocalPublisher struct {
	labels labels.ShardLabeling
}

var _ Publisher = &LocalPublisher{}

func NewLocalPublisher(labels labels.ShardLabeling) *LocalPublisher {
	if labels.LogLevel == po.Log_undefined_level {
		labels.LogLevel = logrusLogLevel()
	}
	return &LocalPublisher{labels}
}

func (p *LocalPublisher) Labels() labels.ShardLabeling { return p.labels }

func (*LocalPublisher) PublishLog(log Log) {
	var level logrus.Level
	switch log.Level {
	case po.Log_trace:
		level = logrus.TraceLevel
	case po.Log_debug:
		level = logrus.DebugLevel
	case po.Log_info:
		level = logrus.InfoLevel
	case po.Log_warn:
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

	if log.Shard.Name != "" && fields["task"] == nil {
		fields["task"] = log.Shard.Name
	}
	logger.WithFields(fields).Log(level, log.Message)
}

// PublishStats implements Publisher
func (*LocalPublisher) PublishStats(event StatsEvent) {
	logrus.WithField("stats", event).Error("got local stats event")
}

// logrusLogLevel maps the current Level of the logrus logger into a pf.LogLevel.
func logrusLogLevel() po.Log_Level {
	switch logrus.StandardLogger().Level {
	case logrus.TraceLevel:
		return po.Log_trace
	case logrus.DebugLevel:
		return po.Log_debug
	case logrus.InfoLevel:
		return po.Log_info
	case logrus.WarnLevel:
		return po.Log_warn
	default:
		return po.Log_error
	}
}
