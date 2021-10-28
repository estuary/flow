package flow

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"

	log "github.com/sirupsen/logrus"
)

// JSWorker wraps a running JavaScript worker process.
type JSWorker struct {
	cmd        *exec.Cmd
	tempdir    string
	socketPath string
}

// NewJSWorker starts a JavaScript worker in the given directory,
// using the given NPM package.
func NewJSWorker(packageTgz []byte) (*JSWorker, error) {
	tempdir, err := ioutil.TempDir("", "javascript-worker")
	if err != nil {
		return nil, fmt.Errorf("creating temp directory: %w", err)
	}
	var socketPath = path.Join(tempdir, "socket")

	var packagePath = path.Join(tempdir, "npm-package.tgz")
	err = ioutil.WriteFile(packagePath, packageTgz, 0600)
	if err != nil {
		return nil, fmt.Errorf("writing package file: %w", err)
	}

	// Bootstrap a Node package with the installed pack.
	var cmd = exec.Command("npm", "install", packagePath)
	cmd.Dir = tempdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	if err = cmd.Run(); err != nil {
		return nil, fmt.Errorf("install NPM package: %w", err)
	}

	// Spawn the worker.
	cmd, err = StartCmdAndReadReady(tempdir, socketPath,
		true, // Place in own process group, to not propagate terminal signals.
		"node_modules/.bin/catalog-js-transformer")
	if err != nil {
		return nil, fmt.Errorf("starting catalog-js-transformer: %w", err)
	}

	return &JSWorker{
		cmd:        cmd,
		tempdir:    tempdir,
		socketPath: socketPath,
	}, nil
}

// Stop gracefully stops the worker process.
func (worker *JSWorker) Stop() error {
	if worker.cmd == nil {
		return nil
	} else if err := worker.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signaling worker: %w", err)
	}
	// Wait will return an error indicating the process was signalled.
	_ = worker.cmd.Wait()

	if err := os.RemoveAll(worker.tempdir); err != nil {
		return fmt.Errorf("cleaning up temp directory: %w", err)
	}

	log.WithFields(log.Fields{
		"args":       worker.cmd.Args,
		"socketPath": worker.socketPath,
		"pid":        worker.cmd.Process.Pid,
	}).Info("stopped worker daemon")

	return nil
}

// StartCmdAndReadReady starts the Cmd blocks until it prints "READY\n" to stderr.
func StartCmdAndReadReady(dir, socketPath string, setpgid bool, args ...string) (*exec.Cmd, error) {
	var cmd = exec.Command(args[0], args[1:]...)
	_ = os.Remove(socketPath)

	var readyCh = make(chan error)

	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "SOCKET_PATH="+socketPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = &readyWriter{delegate: os.Stderr, ch: readyCh}

	// Deliver a SIGTERM to the process if this thread should die uncleanly.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	// Place child its own process group so that terminal SIGINT isn't
	// delivered from the terminal.
	cmd.SysProcAttr.Setpgid = setpgid

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cmd.Start: %w", err)
	}

	log.WithFields(log.Fields{
		"args":       cmd.Args,
		"socketPath": socketPath,
		"pid":        cmd.Process.Pid,
	}).Info("started worker daemon")

	if err := <-readyCh; err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}

	return cmd, nil
}

type readyWriter struct {
	delegate io.Writer
	ch       chan error
}

func (w *readyWriter) Write(p []byte) (int, error) {
	if w.ch == nil {
		return w.delegate.Write(p) // Common case.
	}

	defer func() {
		close(w.ch)
		w.ch = nil
	}()

	if bytes.HasPrefix(p, []byte("READY\n")) {
		var n, err = w.delegate.Write(p[6:])
		n += 6
		return n, err
	} else {
		w.ch <- fmt.Errorf("did not read READY from subprocess")
		return w.delegate.Write(p)
	}
}
