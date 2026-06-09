package runtime

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/estuary/flow/go/protocols/ops"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestMaybeForwardObservable(t *testing.T) {
	// Redirect the standard logger into a buffer using the JSON formatter, so we
	// can make stable assertions on what (if anything) was forwarded.
	var logger = logrus.StandardLogger()
	var origOut, origFormatter, origLevel = logger.Out, logger.Formatter, logger.Level
	t.Cleanup(func() {
		logger.SetOutput(origOut)
		logger.SetFormatter(origFormatter)
		logger.SetLevel(origLevel)
	})

	var buf bytes.Buffer
	logger.SetOutput(&buf)
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	var newPublisher = func() *OpsPublisher {
		var p = &OpsPublisher{}
		p.labels.TaskName = "acmeCo/test/task"
		return p
	}
	var log = func(level ops.Log_Level, message string, observable bool) ops.Log {
		var fields = map[string]json.RawMessage{}
		if observable {
			fields[observableField] = json.RawMessage("true")
		}
		return ops.Log{Level: level, Message: message, FieldsJsonMap: fields}
	}

	t.Run("marked line is forwarded and tagged with task", func(t *testing.T) {
		buf.Reset()
		newPublisher().maybeForwardObservable(log(ops.Log_info, "hello", true))
		require.Contains(t, buf.String(), `"msg":"hello"`)
		require.Contains(t, buf.String(), `"task":"acmeCo/test/task"`)
	})

	t.Run("unmarked line is not forwarded", func(t *testing.T) {
		buf.Reset()
		newPublisher().maybeForwardObservable(log(ops.Log_info, "hello", false))
		require.Empty(t, buf.String())
	})

	t.Run("observable:false is not forwarded", func(t *testing.T) {
		buf.Reset()
		var l = log(ops.Log_info, "hello", false)
		l.FieldsJsonMap[observableField] = json.RawMessage("false")
		newPublisher().maybeForwardObservable(l)
		require.Empty(t, buf.String())
	})

	t.Run("respects the logger's level", func(t *testing.T) {
		buf.Reset()
		// Logger is at Info; a marked debug line must be filtered out.
		newPublisher().maybeForwardObservable(log(ops.Log_debug, "hello", true))
		require.Empty(t, buf.String())
	})
}
