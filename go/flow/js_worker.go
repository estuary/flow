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
	Cmd        *exec.Cmd
	Tempdir    string
	SocketPath string
}

// NewJSWorker starts a JavaScript worker in the given directory,
// using the given NPM package.
func NewJSWorker(catalog *Catalog, overrideSocket string) (*JSWorker, error) {
	if overrideSocket != "" {
		return &JSWorker{SocketPath: overrideSocket}, nil
	}

	tempdir, err := ioutil.TempDir("", "javascript-worker")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	var socketPath = path.Join(tempdir, "socket")

	packageTgz, err := catalog.LoadNPMPackage()
	if err != nil {
		return nil, fmt.Errorf("loading NPM package: %w", err)
	}

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
	cmd = exec.Command("node_modules/.bin/catalog-js-transformer")
	cmd.Dir = tempdir
	cmd.Env = append(os.Environ(), "SOCKET_PATH="+socketPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	log.WithField("args", cmd.Args).Info("starting worker")

	if err = StartCmdAndReadReady(cmd); err != nil {
		return nil, fmt.Errorf("failed to start catalog-js-transformer: %w", err)
	}

	return &JSWorker{
		Cmd:        cmd,
		Tempdir:    tempdir,
		SocketPath: socketPath,
	}, nil
}

// Stop gracefully stops the flow-worker process.
func (worker *JSWorker) Stop() error {
	if worker.Cmd == nil {
		return nil
	} else if err := worker.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to TERM worker: %w", err)
	} else if err = worker.Cmd.Wait(); err != nil {
		return fmt.Errorf("failed to wait for TERM'd worker: %w", err)
	} else if err = os.RemoveAll(worker.Tempdir); err != nil {
		return fmt.Errorf("failed to clean up temp directory: %w", err)
	}
	return nil
}

// StartCmdAndReadReady starts the Cmd blocks until it prints "READY\n" to stderr.
func StartCmdAndReadReady(cmd *exec.Cmd) error {
	// Deliver a SIGTERM to the process if this thread should die uncleanly.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	// Place child its own process group so that terminal SIGINT isn't
	// delivered from the terminal.
	cmd.SysProcAttr.Setpgid = true

	var realStdErr io.Writer
	realStdErr, cmd.Stderr = cmd.Stderr, nil

	if realStdErr == nil {
		realStdErr = ioutil.Discard
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("cmd.StderrPipe: %w", err)
	} else if err = cmd.Start(); err != nil {
		return fmt.Errorf("cmd.Start: %w", err)
	}

	var br = bufio.NewReader(stderr)
	if ready, err := br.ReadString('\n'); err != nil {
		return fmt.Errorf("attempting to read READY: %w", err)
	} else if ready != "READY\n" {
		return fmt.Errorf("wanted READY from subprocess but got %q", ready)
	}
	// Hereafter, shunt stderr output directly to our own handle.
	go io.Copy(realStdErr, br)

	return nil
}
