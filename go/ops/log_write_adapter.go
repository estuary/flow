package ops

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/sirupsen/logrus"
)

// NewLogWriteAdapter returns an io.Writer into which canonical newline-delimited,
// JSON-encoded Logs may be written. As each such log is written into the Writer,
// it's parsed and dispatched to the wrapped Publisher.
// Read logs are not expected to have the `Shard` field populated and an appropriate
// ShardRef will be attached by this adapter based on the Publisher's configured labeling.
func NewLogWriteAdapter(publisher Publisher) io.Writer {
	return &writeAdapter{
		publisher: publisher,
		shard:     NewShardRef(publisher.Labels()),
		rem:       nil,
	}
}

type writeAdapter struct {
	publisher Publisher
	shard     ShardRef
	rem       []byte
}

func (o *writeAdapter) Write(p []byte) (int, error) {
	var n = len(p)

	var newlineIndex = bytes.IndexByte(p, '\n')
	for newlineIndex >= 0 {
		var line = p[:newlineIndex]
		if len(o.rem) > 0 {
			line = append(o.rem, line...)
		}

		var log Log
		if err := json.Unmarshal(line, &log); err != nil {
			// We log but swallow an error because `writeAdapter` is used in contexts where
			// a returned error cannot reasonable cancel an operation underway. We instead
			// let it run and ensure we're at least getting logging of malformed lines.
			logrus.WithFields(logrus.Fields{
				"error": err,
				"line":  string(line),
			}).Error("failed to unmarshal operations log")
		} else {
			log.Shard = o.shard
			o.publisher.PublishLog(log)
		}

		p = p[newlineIndex+1:]
		o.rem = o.rem[:0]
		newlineIndex = bytes.IndexByte(p, '\n')
	}

	if len(o.rem)+len(p) > maxLogSize {
		// As with an unmarshal error, swallow but noisily log that this is happening.
		logrus.WithField("length", len(o.rem)+len(p)).Error("operations log line is too long (discarding)")
		o.rem = o.rem[:0]
	} else if len(p) > 0 {
		// Preserve any remainder of p, since another newline is expected in a subsequent Write.
		o.rem = append(o.rem, p...)
	}

	return n, nil
}

// maxLogLine is the maximum allowable length of any single log line that we will try to parse.
// If logging output contains a sequence longer than this without a newline character, then it will
// be broken up into chunks of this size, which are then processed as normal. The actual value here
// was chosen somewhat arbitrarily.
const maxLogSize = 1 << 20 // 1MB.
