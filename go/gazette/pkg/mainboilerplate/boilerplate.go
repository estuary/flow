// Package mainboilerplate contains shared boilerplate for this project's
// programs. The idea is to provide a selection of narrowly scoped methods so
// callers do not have to buy-in to an all-or-nothing approach.
//
// Importing this package will automatically register net/http/pprof HTTP
// handlers.
package mainboilerplate

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/LiveRamp/gazette/pkg/envflag"
	"github.com/LiveRamp/gazette/pkg/envflagfactory"
)

const (
	// k8sTerminationLog is the location to write a termination message for
	// Kubernetes to retrieve.
	//
	// Link: https://kubernetes.io/docs/tasks/debug-application-cluster/determine-reason-pod-failure/#setting-the-termination-log-file
	k8sTerminationLog = "/dev/termination-log"

	// maxStackTraceSize is the max bytes to allocate to stack traces
	maxStackTraceSize = 32768
)

// Initialize performs an opinionated program initialization. The intention is
// for this to be one-size-fits-all for gazette programs with each program
// specifying additional flags before calling this.
func Initialize() {
	var logLevel = envflagfactory.NewLogLevel()
	var metricsPort = envflagfactory.NewMetricsPort()
	var metricsPath = envflagfactory.NewMetricsPath()

	initFlags()
	initLog(*logLevel)
	initMetrics(*metricsPort, *metricsPath)
	RegisterSignalHandlers()
}

// initFlags parses flags from the environment and command line, in that order.
// This should be one of the first things a program does to ensure the provided
// configuration takes effect.
func initFlags() {
	envflag.CommandLine.Parse()
	flag.Parse()
}

// initLog configures the logger.
func initLog(level string) {
	log.SetFormatter(&log.JSONFormatter{})
	if lvl, err := log.ParseLevel(level); err != nil {
		log.WithField("err", err).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
		log.WithField("level", lvl).Debug("using log level")
	}
}

// initMetrics enables serving of metrics over the given port and path.
func initMetrics(port, path string) {
	http.Handle(path, promhttp.Handler())
	go http.ListenAndServe(port, nil)
}

// LogPanic is intended to be a deferred call to log a panic at the end of the
// program's lifecycle.
func LogPanic() {
	if r := recover(); r != nil {
		logTerminationMessage(fmt.Sprint("PANIC: ", r))
		logStackTrace(r)

		// Bubble up the panic.
		panic(r)
	}
}

func logTerminationMessage(msg string) {
	// Make a best effort attempt to write a termination message.
	//
	// Bug: https://github.com/kubernetes/kubernetes/issues/31839
	if f, err := os.OpenFile(k8sTerminationLog, os.O_WRONLY, 0777); err != nil {
		defer f.Close()
		f.WriteString(msg)
	}
}

func logStackTrace(r interface{}) {
	var stack = make([]byte, maxStackTraceSize)
	stack = stack[:runtime.Stack(stack, true)]
	log.WithFields(log.Fields{
		"err":   r,
		"stack": strings.Split(string(stack), "\n"),
	}).Fatal("panic")
}
