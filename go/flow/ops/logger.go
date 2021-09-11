package ops

import (
	log "github.com/sirupsen/logrus"
)

// LogPublisher is an interface for publishing log messages that relate to a specific task.
// This is used so that logs can be published to Flow ops collections at runtime, but can be
// written to stderr at build/apply time.
type LogPublisher interface {
	Log(level log.Level, fields log.Fields, message string) error
}

type stdLogPublisher struct{}

func (stdLogPublisher) Log(level log.Level, fields log.Fields, message string) error {
	log.WithFields(fields).Log(level, message)
	return nil
}

// StdLogPublisher returns a LogPublisher that just forwards to the logrus package. This is used
// during operations that happen outside of the Flow runtime (such as flowctl build or apply).
func StdLogPublisher() LogPublisher {
	return stdLogPublisher{}
}
