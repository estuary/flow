package flow

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"syscall"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// WorkerHost hosts a wrapped, running flow-worker process.
type WorkerHost struct {
	Cmd  *exec.Cmd
	Conn *grpc.ClientConn
}

// NewWorkerHost starts a flow-worker process with the given arguments.
func NewWorkerHost(args ...string) (*WorkerHost, error) {
	var socketFile, err = ioutil.TempFile("", "flow-grpc-socket")
	if err != nil {
		return nil, fmt.Errorf("failed to create tempfile: %w", err)
	}
	var socketPath = socketFile.Name()

	_ = socketFile.Close()
	if err = os.Remove(socketPath); err != nil {
		return nil, fmt.Errorf("failed to remove tempfile: %w", err)
	}

	var cmd = exec.Command("flow-worker", append(args, "--grpc-socket-path", socketPath)...)
	cmd.Stderr = os.Stderr

	// Start flow-worker, and expect to read "READY\n" from it's stdout
	// once it's started and has bound the unix socket we provided.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe for flow-worker: %w", err)
	} else if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start flow-worker: %w", err)
	}
	log.WithField("args", cmd.Args).Info("started flow-worker")

	var br = bufio.NewReader(stdout)
	if ready, err := br.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("failed to read READY from flow-worker: %w", err)
	} else if ready != "READY\n" {
		return nil, fmt.Errorf("unexpected READY from flow-worker: %q", ready)
	}
	go br.WriteTo(os.Stdout) // Forward future stdout content.

	conn, err := grpc.DialContext(context.Background(), "unix://"+socketPath, grpc.WithBlock(), grpc.WithInsecure(), grpc.WithAuthority("localhost"))
	if err != nil {
		return nil, fmt.Errorf("failed to dial flow-worker: %w", err)
	}

	return &WorkerHost{
		Cmd:  cmd,
		Conn: conn,
	}, nil
}

// Stop gracefully stops the flow-worker process.
func (wh *WorkerHost) Stop() error {
	if err := wh.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to TERM flow-worker: %w", err)
	} else if err = wh.Cmd.Wait(); err != nil {
		return fmt.Errorf("failed to wait for TERM'd flow-worker: %w", err)
	}
	return nil
}
