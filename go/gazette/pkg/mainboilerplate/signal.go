package mainboilerplate

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
)

var (
	traceEnabled     bool
	previousLogLevel log.Level
)

// RegisterSignalHandlers registers signal handlers for debugging and
// profiling.
//
// SIGQUIT
//   Dump a one-time heap and goroutine trace to stdout.
//
// SIGUSR1
//   Start a long-running CPU profile using pprof. The profile is written to
//   /var/tmp/profile_${PID}_${TIMESTAMP}.pprof where TIMESTAMP is the epoch
//   time when the profiling session began. Sending SIGUSR1 again will stop the
//   profiling and flush writes for the profile.
//
// SIGUSR2
//   Toggle debug log level.
func RegisterSignalHandlers() {
	notifyChan := make(chan os.Signal, 1)
	signal.Notify(notifyChan, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for {
			switch <-notifyChan {
			case syscall.SIGQUIT:
				dump(os.Stdout)
			case syscall.SIGUSR1:
				toggleProfiler()
			case syscall.SIGUSR2:
				toggleTrace()
			}
		}
	}()
}

// toggleTrace toggles debug logging
func toggleTrace() {
	if traceEnabled {
		log.SetLevel(previousLogLevel)
	} else {
		previousLogLevel = log.GetLevel()
		log.SetLevel(log.DebugLevel)
	}
	traceEnabled = !traceEnabled
}
