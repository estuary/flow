package ops

import (
	"encoding/json"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/estuary/flow/go/flow/ops/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

//func TestLogLevelUnmarshaling(t *testing.T) {
//	var testCases = []struct {
//		input     string
//		expect    pf.LogLevelFilter
//		expectErr bool
//	}{
//		{input: `"inFormation"`, expect: pf.LogLevelFilter_RAW},
//		{input: `"info"`, expect: pf.LogLevelFilter_INFO},
//		{input: `"INFO"`, expect: pf.LogLevelFilter_INFO},
//		{input: `"WARN"`, expect: pf.LogLevelFilter_WARN},
//		{input: `"warning"`, expect: pf.LogLevelFilter_WARN},
//		{input: `"Trace"`, expect: pf.LogLevelFilter_TRACE},
//		// This is just documenting the weird edge case.
//		{input: `"Trace a line in the sand"`, expect: pf.LogLevelFilter_TRACE},
//		{input: `"FATAL"`, expect: pf.LogLevelFilter_RAW},
//		{input: `"panic"`, expect: pf.LogLevelFilter_RAW},
//		{input: `{ "level": "info" }`, expectErr: true},
//		{input: `"not a real level"`, expectErr: true},
//		{input: `4`, expectErr: true},
//	}
//
//	for _, testCase := range testCases {
//		var actual, ok = parseLogLevel([]byte(testCase.input))
//		if testCase.expectErr {
//			require.Falsef(t, ok, "case failed: %+v", testCase)
//		} else {
//			require.Truef(t, ok, "parsing level failed: %+v", testCase)
//		}
//		require.Equalf(t, testCase.expect, actual, "mismatched: %+v, actual: %v", testCase, actual)
//	}
//}

func TestLogEventUnmarshaling(t *testing.T) {
	var doTest = func(line string, expected testutil.TestLogEvent) {
		var actual logEvent
		require.NoError(t, json.Unmarshal([]byte(line), &actual), "failed to parse line:", line)

		var actualEvent = testutil.TestLogEvent{
			Level:     pf.LogLevelFilter(actual.Level),
			Timestamp: actual.Timestamp,
			Message:   actual.Message,
			Fields:    testutil.NormalizeFields(actual.Fields),
		}
		require.Truef(t, expected.Matches(&actualEvent), "mismatched event for line: %s, expected: %+v, actual: %+v", line, expected, actualEvent)
	}

	doTest(
		`{"lvl": "info", "msg": "foo", "ts": "2021-09-10T12:01:07.01234567Z"}`,
		testutil.TestLogEvent{
			Level:     pf.LogLevelFilter_INFO,
			Message:   "foo",
			Timestamp: timestamp("2021-09-10T12:01:07.01234567Z"),
		},
	)
	doTest(
		`{"level": "TRace", "message": "yea boi", "fieldA": "valA", "ts": "2021-09-10T12:01:06.01234567Z"}`,
		testutil.TestLogEvent{
			Level:     pf.LogLevelFilter_TRACE,
			Message:   "yea boi",
			Timestamp: timestamp("2021-09-10T12:01:06.01234567Z"),
			Fields: map[string]interface{}{
				"fieldA": "valA",
			},
		},
	)
	doTest(
		`{"LVL": "not a real level", "message": {"wat": "huh"}, "fieldA": "valA", "ts": "not a real timestamp"}`,
		testutil.TestLogEvent{
			Fields: map[string]interface{}{
				"fieldA":  "valA",
				"LVL":     "not a real level",
				"ts":      "not a real timestamp",
				"message": map[string]interface{}{"wat": "huh"},
			},
		},
	)
	doTest(
		`{"LVL": "not a real level", "LEVEL": "also not a real level", "level": "info", "message": {"wat": "huh"}, "fieldA": "valA", "ts": "not a real timestamp", "msg": "the real message"}`,
		testutil.TestLogEvent{
			Level: pf.LogLevelFilter_INFO,
			Fields: map[string]interface{}{
				"fieldA":  "valA",
				"LVL":     "not a real level",
				"LEVEL":   "also not a real level",
				"ts":      "not a real timestamp",
				"message": map[string]interface{}{"wat": "huh"},
			},
			Message: "the real message",
		},
	)
	doTest(`{}`, testutil.TestLogEvent{})
}

func TestLogForwardWriterWhenDataHasNoNewlines(t *testing.T) {
	const maxLogLine = 65536
	// We'll write more than the max line length, and assert that the writer breaks it into chunks
	// at the max line length. We'll then assert that the remaining 999 bytes get logged at the end.
	var rawLogs = strings.Repeat("f", maxLogLine*2+999)
	var publisher = testutil.NewTestLogPublisher(log.TraceLevel)
	var sourceDesc = "naughty stderr"
	var fallbackLevel = pf.LogLevelFilter_RAW
	var writer = NewLogForwardWriter(sourceDesc, publisher)

	// Read from rawLogs in a bunch of random small chunks to ensure that the writer is piecing the
	// lines together correctly.
	var n int
	for n < len(rawLogs) {
		var nextLen = rand.Intn(20)
		if len(rawLogs)-n < nextLen {
			nextLen = len(rawLogs) - n
		}
		var slice = ([]byte(rawLogs)[n : n+nextLen])
		n = n + nextLen
		var w, err = writer.Write(slice)
		require.NoError(t, err)
		require.Equal(t, nextLen, w)
	}

	require.NoError(t, writer.Close())

	var expected = []testutil.TestLogEvent{
		{
			Level:   fallbackLevel,
			Message: strings.Repeat("f", maxLogLine),
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
			},
		},
		{
			Level:   fallbackLevel,
			Message: strings.Repeat("f", maxLogLine),
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
			},
		},
		{
			Level:   fallbackLevel,
			Message: strings.Repeat("f", 999),
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
			},
		},
		{
			Level:   pf.LogLevelFilter_TRACE,
			Message: "finished forwarding logs",
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
				"textLines": 3,
			},
		},
	}

	publisher.RequireEventsMatching(t, expected)

}

func TestLogForwarding(t *testing.T) {
	// Pass the same input to both LogForwardWriter and ForwardLogs, and assert that we get the same
	// log events as output.
	// The raw logs contains empty lines and trailing whitespace, which should be trimmed off.
	var rawLogs = `
{"level": "TRace", "message": "yea boi", "fieldA": "valA", "ts": "2021-09-10T12:01:06.01234567Z"}
{"lVl": "iNfO", "MSG": "infoMessage", "fieldA": "valA", "ts": "2021-09-10T12:01:07.01234567Z"}


{"lEVEl": "warning", "Message": "warnMessage", "fieldA": "warnValA", "TimeStamp": "2021-09-10T12:01:08.01234567Z"}
2021-09-10T12:01:09.456Z INFO some text
{"foo": "bar"}
 a b c
 {"Lvl": "not even close to a real level"}`

	var sourceDesc = "testSource"
	var expected = []testutil.TestLogEvent{
		{
			Level:   pf.LogLevelFilter_TRACE,
			Message: "yea boi",
			Fields: map[string]interface{}{
				"fieldA":    "valA",
				"logSource": sourceDesc,
			},
			Timestamp: timestamp("2021-09-10T12:01:06.01234567Z"),
		},
		{
			Level:   pf.LogLevelFilter_INFO,
			Message: "infoMessage",
			Fields: map[string]interface{}{
				"fieldA":    "valA",
				"logSource": sourceDesc,
			},
			Timestamp: timestamp("2021-09-10T12:01:07.01234567Z"),
		},
		{
			Level:   pf.LogLevelFilter_WARN,
			Message: "warnMessage",
			Fields: map[string]interface{}{
				"fieldA":    "warnValA",
				"logSource": sourceDesc,
			},
			Timestamp: timestamp("2021-09-10T12:01:08.01234567Z"),
		},
		{
			Level:   pf.LogLevelFilter_INFO,
			Message: "2021-09-10T12:01:09.456Z INFO some text",
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
			},
		},
		{
			Level:   pf.LogLevelFilter_RAW,
			Message: "",
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
				"foo":       "bar",
			},
		},
		{
			Level:   pf.LogLevelFilter_RAW,
			Message: " a b c",
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
			},
		},
		{
			Level:   pf.LogLevelFilter_RAW,
			Message: "",
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
				"Lvl":       "not even close to a real level",
			},
		},
		{
			Level:   pf.LogLevelFilter_RAW,
			Message: "finished forwarding logs",
			Fields: map[string]interface{}{
				"logSource": sourceDesc,
				"jsonLines": 5,
				"textLines": 2,
			},
		},
	}

	t.Run("LogForwardWriter", func(t *testing.T) {
		var publisher = testutil.NewTestLogPublisher(log.TraceLevel)
		var writer = NewLogForwardWriter(sourceDesc, publisher)

		// Read from rawLogs in a bunch of random small chunks to ensure that the writer is piecing the
		// lines together correctly.
		var n int
		for n < len(rawLogs) {
			var nextLen = rand.Intn(20)
			if len(rawLogs)-n < nextLen {
				nextLen = len(rawLogs) - n
			}
			var slice = ([]byte(rawLogs)[n : n+nextLen])
			n = n + nextLen
			var w, err = writer.Write(slice)
			require.NoError(t, err)
			require.Equal(t, nextLen, w)
		}
		require.NoError(t, writer.Close())

		publisher.RequireEventsMatching(t, expected)
	})

	t.Run("ForwardLogs", func(t *testing.T) {
		var publisher = testutil.NewTestLogPublisher(log.TraceLevel)
		ForwardLogs(sourceDesc, io.NopCloser(strings.NewReader(rawLogs)), publisher)
		publisher.RequireEventsMatching(t, expected)
	})
}

func timestamp(strVal string) time.Time {
	var t, err = time.Parse(time.RFC3339, strVal)
	if err != nil {
		panic(err)
	}
	return t
}
