package testutil

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	//"github.com/stretchr/testify/assert"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// TestLogEvent represents either a log event that has been written to a TestLogPublisher, or an
// expected event to match against.
type TestLogEvent struct {
	Timestamp time.Time
	Level     pf.LogLevelFilter
	Message   string
	Fields    map[string]interface{}
}

// Matches asserts that the |actual| log event matches the |expected| receiver.
// If the expected timestamp is zero, then no check will be done on the timestamp.
// Extra fields are allowed on the actual event, and only the fields present on the expected event
// will be checked.
func (expected *TestLogEvent) Matches(actual *TestLogEvent) bool {
	if actual == nil {
		return expected == nil
	}
	if expected.Level != actual.Level || expected.Message != actual.Message {
		return false
	}
	if !expected.Timestamp.IsZero() && expected.Timestamp.Format(time.RFC3339Nano) != actual.Timestamp.Format(time.RFC3339Nano) {
		return false
	}
	for key, expectedField := range expected.Fields {
		var actualField, ok = actual.Fields[key]
		if !ok {
			return false
		}
		var expectedJson, err = json.Marshal(&expectedField)
		if err != nil {
			panic(err)
		}
		actualJson, err := json.Marshal(&actualField)
		if err != nil {
			panic(err)
		}

		if string(expectedJson) != string(actualJson) {
			return false
		}
	}
	return true
}

func NormalizeFields(fields interface{}) map[string]interface{} {
	var fieldsJson, err = json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	var m = make(map[string]interface{})
	err = json.Unmarshal(fieldsJson, &m)
	if err != nil {
		panic(err)
	}
	return m
}

// TestLogPublisher is an ops.LogPublisher that collects all log events in memory and allows for
// assertions that they match expected events.
type TestLogPublisher struct {
	mutex  sync.Mutex
	events []TestLogEvent
	level  pf.LogLevelFilter
}

func NewTestLogPublisher(level pf.LogLevelFilter) *TestLogPublisher {
	return &TestLogPublisher{
		level: level,
	}
}

// WaitForLogs waits at most |timeout| for |logCount| number of log events to be available. If the
// expected number of events have not been logged prior to the |timeout| expiration, this will fail
// the test immediately.
func (p *TestLogPublisher) WaitForLogs(t *testing.T, timeout time.Duration, logCount int) {
	var deadline = time.Now().Add(timeout)
	var n int
	for time.Now().Before(deadline) {
		p.mutex.Lock()
		n = len(p.events)
		p.mutex.Unlock()
		if n >= logCount {
			return
		}
	}
	var events = p.TakeEvents()
	require.FailNowf(t, "WaitForLogs failed", "timed out after %s waiting on %d logs, only got %d: %+v", timeout.String(), logCount, n, events)
}

// RequireEventsMatching requires that the |expected| events have been logged. It performs this
// check immediately and fails the test if any events do not match, or if the number of expected and
// actual log events is not the same. This function consumes all of the current events, so
// subsequent calls to RequireEventsMatching will fail.
func (p *TestLogPublisher) RequireEventsMatching(t *testing.T, expected []TestLogEvent) {
	var actual = p.TakeEvents()

	for i, expectedEvent := range expected {
		if len(actual) <= i {
			break // error will have been reported by the length check
		}
		if !expectedEvent.Matches(&actual[i]) {
			require.Failf(t, "mismatched event", "event %d mismatched, expected: %+v, actual: %+v", i, expectedEvent, actual[i])
		}
	}
	if len(actual) > len(expected) {
		require.Failf(t, "more actual logs than expected", "Extra actual: %+v", actual[len(expected):])
	} else if len(actual) < len(expected) {
		require.Failf(t, "more expected logs than actual", "Extra expected: %+v", expected[len(actual):])
	}
}

// Immediately take all of the events that have been logged up until this point.
func (p *TestLogPublisher) TakeEvents() []TestLogEvent {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var events = p.events
	p.events = nil
	return events
}

func (p *TestLogPublisher) Level() pf.LogLevelFilter {
	return p.level
}

func (p *TestLogPublisher) Log(level pf.LogLevelFilter, fields log.Fields, message string) error {
	if level > p.level {
		return nil
	}
	var event = TestLogEvent{
		Timestamp: time.Now().UTC(),
		Level:     level,
		Message:   message,
		Fields:    NormalizeFields(fields),
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.events = append(p.events, event)
	return nil
}

func (p *TestLogPublisher) LogForwarded(ts time.Time, level pf.LogLevelFilter, fields map[string]json.RawMessage, message string) error {
	if level > p.level {
		return nil
	}
	var event = TestLogEvent{
		Timestamp: ts,
		Level:     level,
		Message:   message,
		Fields:    NormalizeFields(fields),
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.events = append(p.events, event)
	return nil
}
