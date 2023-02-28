package connector

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/pkgbin"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// StartFirecracker starts a firecracker VM of the specified connector image.
// The VM will run until the provided context is complete,
// at which point it will be torn down.
func StartFirecracker(
	ctx context.Context,
	image string,
	publisher ops.Publisher,
) (*Container, error) {
	// Don't undertake expensive operations if we're already shutting down.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Copy flow-connector-init to $TMPDIR, from where it may be mounted into the connector.
	connectorInitPath, locateError := pkgbin.Locate(flowConnectorInit)
	if locateError != nil {
		return nil, fmt.Errorf("finding %q binary: %w", flowConnectorInit, locateError)
	}

	// Copy flow-connector-init to $TMPDIR, from where it may be mounted into the connector.
	kernelPath, kernelPathError := pkgbin.Locate(kernelName)
	if kernelPathError != nil {
		return nil, fmt.Errorf("finding %q kernel: %w", kernelName, kernelPathError)
	}

	// Copy flow-connector-init to $TMPDIR, from where it may be mounted into the connector.
	flowFirecrackerPath, flowFirecrackerError := pkgbin.Locate(flowFirecracker)
	if flowFirecrackerError != nil {
		return nil, fmt.Errorf("finding %q: %w", flowFirecracker, flowFirecrackerError)
	}

	// Port on which connector-init listens for requests.
	// This is the default, made explicit here.
	const guestPort = 8080
	// Mapped and published connector-init port accessible from the host.
	var hostPort, err = GetFreePort()
	if err != nil {
		return nil, fmt.Errorf("allocating connector host port: %w", err)
	}

	var labels = publisher.Labels()
	var args = []string{
		flowFirecrackerPath,
		"--init-program", connectorInitPath,
		"--kernel", kernelPath,
		"--image-name", image,
		// Publish the flow-connector-init port through to a mapped host port.
		"--publish", fmt.Sprintf("%d:%d/tcp", hostPort, guestPort),
		// Thread-through the logging configuration of the connector.
		"--env", "LOG_FORMAT=json",
		"--env", "LOG_LEVEL=" + labels.LogLevel.String(),
		// Cgroup memory / CPU resource limits.
		// TODO(johnny): we intend to tighten these down further, over time.
		"--memory", "1024",
		"--cpus", "2",
		"--attach", "--raw-vm-logs",
		"--log-format=json",
		// TODO: Setup matrics with labels and metrics
		// "--label", fmt.Sprintf("build=%s", labels.Build),
		// "--label", fmt.Sprintf("image=%s", image),
		// "--label", fmt.Sprintf("task-name=%s", labels.TaskName),
		// "--label", fmt.Sprintf("task-type=%s", labels.TaskType),
		// The following are arguments of connector-init, not docker.
		"--",
		"--port", fmt.Sprint(guestPort),
	}

	logrus.WithFields(logrus.Fields{"args": args}).Debug("invoking flow-firecracker")

	// `cmdCtx` has a scope equal to the lifetime of the container.
	// It's cancelled with the parent context, or when the container crashes,
	// or when we wish it to gracefully exit.
	var cmdCtx, cmdCancel = context.WithCancel(ctx)
	defer func() {
		if cmdCancel != nil {
			cmdCancel()
		}
	}()

	// We use Command instead of CommandContext because we send a SIGTERM
	// rather than a SIGKILL on `cmdCtx` cancellation.
	var cmd = exec.Command(args[0], args[1:]...)

	cmd.Stderr = ops.NewLogWriteAdapter(publisher)
	cmd.Stdin = nil
	cmd.Stdout = ops.NewLogWriteAdapter(publisher)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting connector: %w", err)
	}

	// Start an asynchronous cmd.Wait(). When `cmd` exits, immediately cancel
	// `cmdCtx` to abort any (likely blocking) RPC attempts to the container.
	// Note that in the garden path `cmdCtx` will already have been cancelled
	// prior to Wait returning, but we need to sanely handle a container crash.
	var waitCh = make(chan error)
	go func(cmdCancel context.CancelFunc) {
		var err = cmd.Wait()
		cmdCancel()
		waitCh <- err
		close(waitCh)
	}(cmdCancel)

	// Arrange for `cmd` to be signaled if `cmdCtx` is cancelled.
	// On being signalled, docker will propagate the signal to the container
	// and wait for exit or for its shutdown timeout to elapse (10s default).
	go func() {
		<-cmdCtx.Done()
		if sigErr := cmd.Process.Signal(syscall.SIGTERM); sigErr != nil && sigErr != os.ErrProcessDone {
			logrus.WithError(sigErr).Error("failed to send signal to container process")
		}
	}()

	conn, err := grpc.DialContext(
		cmdCtx,
		fmt.Sprintf("127.0.0.1:%d", hostPort),
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	)
	if err != nil {
		if waitErr := <-waitCh; waitErr != nil {
			err = fmt.Errorf("crashed with %w", waitErr)
		}
		return nil, fmt.Errorf("dialing container connector-init: %w", err)
	}

	logrus.WithFields(logrus.Fields{"image": image, "portHost": hostPort}).Info("started connector")

	var out = &Container{
		conn:      conn,
		cmd:       cmd,
		cmdCancel: cmdCancel,
		image:     image,
		waitCh:    waitCh,
	}

	// Deactivate deferred teardown.
	cmdCancel = nil

	return out, nil
}

const kernelName = "vmlinux.bin"
const flowFirecracker = "flow-firecracker"
