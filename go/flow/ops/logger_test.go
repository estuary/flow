package ops

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/estuary/flow/go/flow/ops/testutil"
	log "github.com/sirupsen/logrus"
)

func TestAddFieldsToLogger(t *testing.T) {
	var testLogger = testutil.NewTestLogPublisher(log.DebugLevel)
	var subject = NewLoggerWithFields(testLogger, log.Fields{
		"coalMine": "canary",
		"foo":      3,
	})

	subject.Log(log.DebugLevel, nil, "one")
	subject.Log(log.TraceLevel, log.Fields{
		"should not": "see this",
	}, "not gonna loggit")
	subject.Log(log.InfoLevel, log.Fields{
		"foo": "not three",
	}, "two")
	var forwardTs = time.Now().UTC()
	subject.LogForwarded(forwardTs, log.WarnLevel, map[string]json.RawMessage{
		"bar": json.RawMessage(`"yarr!"`),
	}, "three")
	subject.LogForwarded(time.Now(), log.TraceLevel, nil, "not gonna log this either")

	var expected = []testutil.TestLogEvent{
		{
			Level: log.DebugLevel,
			Fields: log.Fields{
				"foo":      3,
				"coalMine": "canary",
			},
			Message: "one",
		},
		{
			Level: log.InfoLevel,
			Fields: log.Fields{
				"foo":      "not three",
				"coalMine": "canary",
			},
			Message: "two",
		},
		{
			Level: log.WarnLevel,
			Fields: log.Fields{
				"foo":      3,
				"coalMine": "canary",
				"bar":      "yarr!",
			},
			Message:   "three",
			Timestamp: forwardTs,
		},
	}
	testLogger.RequireEventsMatching(t, expected)
}
