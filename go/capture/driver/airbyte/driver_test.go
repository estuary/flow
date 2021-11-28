package airbyte

import (
	"fmt"
	"testing"

	"github.com/estuary/flow/go/flow/ops/testutil"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestOnStdoutDecodeError(t *testing.T) {
	var publisher = testutil.NewTestLogPublisher(log.DebugLevel)

	var subject = driver{
		Logger: publisher,
	}
	var err = fmt.Errorf("only a test")

	// This line should be logged because it's not json, and the error ignored.
	var result = subject.onStdoutDecodeError([]byte("foo\n"), err)
	require.Nil(t, result)

	publisher.RequireEventsMatching(t, []testutil.TestLogEvent{{
		Level:   log.InfoLevel,
		Message: "foo",
		Fields: map[string]interface{}{
			"logSource": "ignored invalid output from connector stdout",
		},
	}})

	// This line should _not_ be logged because the error should bubble up and get logged
	// by whatever called the connector.
	result = subject.onStdoutDecodeError([]byte("{\"foo\": 123}"), err)
	require.Equal(t, err, result)
	require.Empty(t, publisher.TakeEvents())
}
