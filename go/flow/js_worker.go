package flow

import (
	"bufio"
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
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	var socketPath = path.Join(tempdir, "socket")

	err = ioutil.WriteFile(path.Join(tempdir, "npm-package.tgz"), packageTgz, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to write package file: %w", err)
	}

	// Bootstrap a Node package with the installed pack.
	var cmd = exec.Command("npm", "install", "file://./npm-package.tgz")
	cmd.Dir = tempdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	if err = cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to install NPM package: %w", err)
	}

	// Spawn the worker.
	cmd, err = StartCmdAndReadReady(tempdir, socketPath,
		true, // Place in own process group, to not propagate terminal signals.
		"node_modules/.bin/catalog-js-transformer")
	if err != nil {
		return nil, fmt.Errorf("failed to start catalog-js-transformer: %w", err)
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
		return fmt.Errorf("failed to TERM worker: %w", err)
	} else if err = worker.cmd.Wait(); err != nil {
		return fmt.Errorf("failed to wait for TERM'd worker: %w", err)
	} else if err = os.RemoveAll(worker.tempdir); err != nil {
		return fmt.Errorf("failed to clean up temp directory: %w", err)
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

	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "SOCKET_PATH="+socketPath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	// Deliver a SIGTERM to the process if this thread should die uncleanly.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	// Place child its own process group so that terminal SIGINT isn't
	// delivered from the terminal.
	cmd.SysProcAttr.Setpgid = setpgid

	var realStdErr io.Writer
	realStdErr, cmd.Stderr = cmd.Stderr, nil

	if realStdErr == nil {
		realStdErr = ioutil.Discard
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("cmd.StderrPipe: %w", err)
	} else if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("cmd.Start: %w", err)
	}

	log.WithFields(log.Fields{
		"args":       cmd.Args,
		"socketPath": socketPath,
		"pid":        cmd.Process.Pid,
	}).Info("started worker daemon")

	var br = bufio.NewReader(stderr)
	if ready, err := br.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("attempting to read READY: %w", err)
	} else if ready != "READY\n" {
		return nil, fmt.Errorf("wanted READY from subprocess but got %q", ready)
	}
	// Hereafter, shunt stderr output directly to our own handle.
	go io.Copy(realStdErr, br)

	return cmd, nil
}
