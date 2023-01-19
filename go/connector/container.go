package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/estuary/flow/go/labels"
	"github.com/estuary/flow/go/ops"
	"github.com/estuary/flow/go/pkgbin"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Container is a connector running as a linux container.
type Container struct {
	cmd *exec.Cmd
	// Cancelling cmdCancel sends SIGTERM to `cmd`, causing it to shutdown.
	// It's also cancelled if `cmd` exits prematurely on its own (aka crashes)
	cmdCancel context.CancelFunc
	// Dialed, ready connection to the container's flow-connector-init server.
	conn *grpc.ClientConn
	// Linux container image being run.
	image string
	// Temporaries which are bind-mounted into the container.
	tmpProxy, tmpInspect *os.File
	// waitCh receives the final result of an ongoing cmd.Wait.
	waitCh chan error
}

// StartContainer starts a container of the specified connector image.
// The container will run until the provided context is complete,
// at which point it will be torn down.
//
// Today, this implementation use Docker to run the image.
// In the future we expect to use Firecracker.
//
// StartContainer is docker-from-docker friendly: it does require that
// files written to $TMPDIR may be mounted into and read by the container,
// but does not mount any other paths.
func StartContainer(
	ctx context.Context,
	image string,
	network string,
	publisher ops.Publisher,
	exposePorts map[string]*labels.PortConfig,
) (*Container, error) {
	// Don't undertake expensive operations if we're already shutting down.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var tmpProxy, tmpInspect *os.File

	// Copy flow-connector-init to $TMPDIR, from where it may be mounted into the connector.
	if rPath, err := pkgbin.Locate(flowConnectorInit); err != nil {
		return nil, fmt.Errorf("finding %q binary: %w", flowConnectorInit, err)
	} else if r, err := os.Open(rPath); err != nil {
		return nil, fmt.Errorf("opening %s: %w", rPath, err)
	} else if tmpProxy, err = copyToTempFile(r, 0555); err != nil {
		return nil, fmt.Errorf("copying %s to tmpfile: %w", rPath, err)
	}

	// Cleanup if we fail later with an error.
	defer func() {
		if tmpProxy != nil {
			_ = os.Remove(tmpProxy.Name())
		}
	}()

	// Pull and inspect the image, saving its output for mounting within the container.
	if err := PullImage(ctx, image); err != nil {
		return nil, err
	} else if out, err := InspectImage(ctx, image); err != nil {
		return nil, err
	} else if tmpInspect, err = copyToTempFile(bytes.NewReader(out), 0444); err != nil {
		return nil, fmt.Errorf("writing image inspection output: %w", err)
	}

	defer func() {
		if tmpInspect != nil {
			_ = os.Remove(tmpInspect.Name())
		}
	}()

	// If the image network is undefined, use an explicit of "bridge".
	// This is default `docker run` behavior if --network is not provided.
	if network == "" {
		network = "bridge"
	}

	// Port on which connector-init listens for requests.
	// This is the default, made explicit here.
	const portInit = 8080
	// Mapped and published connector-init port accessible from the host.
	var portHost, err = GetFreePort()
	if err != nil {
		return nil, fmt.Errorf("allocating connector host port: %w", err)
	}

	var labels = publisher.Labels()
	var args = []string{
		"docker",
		"run",
		// Remove the docker container upon its exit.
		"--rm",
		// Network to which the container should attach.
		"--network", network,
		// The entrypoint into a connector is always flow-connector-init,
		// which will delegate to the actual entrypoint of the connector.
		"--entrypoint", "/flow-connector-init",
		// Mount the flow-connector-init binary and `docker inspect` output.
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/flow-connector-init", tmpProxy.Name()),
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/image-inspect.json", tmpInspect.Name()),
		// Publish the flow-connector-init port through to a mapped host port.
		// We use 0.0.0.0 instead of 127.0.0.1 for compatibility with GitHub CodeSpaces.
		"--publish", fmt.Sprintf("0.0.0.0:%d:%d/tcp", portHost, portInit),
		// Thread-through the logging configuration of the connector.
		"--env", "LOG_FORMAT=json",
		"--env", "LOG_LEVEL=" + labels.LogLevel.String(),
		// Cgroup memory / CPU resource limits.
		// TODO(johnny): we intend to tighten these down further, over time.
		"--memory", "1g",
		"--cpus", "2",
		// Attach labels that let us group connector resource usage under a few dimensions.
		"--label", fmt.Sprintf("build=%s", labels.Build),
		"--label", fmt.Sprintf("image=%s", image),
		"--label", fmt.Sprintf("task-name=%s", labels.TaskName),
		"--label", fmt.Sprintf("task-type=%s", labels.TaskType),
		image,
		// The following are arguments of connector-init, not docker.
		"--image-inspect-json-path=/image-inspect.json",
		"--port", fmt.Sprint(portInit),
	}

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
	// rather than a SIGKILL on `cmdCtx` cancellation. Note that docker
	// already has handling for propagating graceful termination with a
	// SIGKILL timeout, that we want to re-use rather than rolling our own.
	var cmd = exec.Command(args[0], args[1:]...)

	cmd.Stderr = ops.NewLogWriteAdapter(publisher)
	cmd.Stdin = nil
	cmd.Stdout = nil

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
		fmt.Sprintf("127.0.0.1:%d", portHost),
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

	logrus.WithFields(logrus.Fields{"image": image, "portHost": portHost}).Info("started connector")

	var out = &Container{
		conn:       conn,
		cmd:        cmd,
		cmdCancel:  cmdCancel,
		image:      image,
		tmpInspect: tmpInspect,
		tmpProxy:   tmpProxy,
		waitCh:     waitCh,
	}

	// Deactivate deferred teardown.
	cmdCancel, tmpInspect, tmpProxy = nil, nil, nil

	return out, nil
}

// Stop the Container, blocking until it fully exits.
func (c *Container) Stop() error {
	var closeErr, waitErr, rmErr1, rmErr2 error
	closeErr = c.conn.Close()

	// Cancel so that the container is signaled and cmd.Wait will eventually complete.
	c.cmdCancel()

	waitErr = <-c.waitCh
	rmErr1 = os.Remove(c.tmpProxy.Name())
	rmErr2 = os.Remove(c.tmpInspect.Name())

	for _, e := range []struct {
		err error
		f   string
	}{
		{closeErr, "closing container gRPC connection: %w"},
		{waitErr, "connector-init crashed: %w"},
		{rmErr1, "removing temporary connector-init file: %w"},
		{rmErr2, "removing temporary inspect file: %w"},
	} {
		if e.err != nil {
			return fmt.Errorf(e.f, e.err)
		}
	}

	logrus.WithField("image", c.image).Info("connector successfully stopped")
	return nil
}

// PullImage to local cache unless the tag is `:local`, which is expected to be local.
func PullImage(ctx context.Context, image string) error {
	// Pull the image if it's not expected to be local.
	if strings.HasSuffix(image, ":local") {
		// Don't pull images having this tag.
	} else if _, err := exec.CommandContext(ctx, "docker", "pull", "--quiet", image).Output(); err != nil {
		return fmt.Errorf("docker pull of container image %q failed: %w", image, err)
	}
	return nil
}

// InspectImage and return its Docker-compatible metadata JSON encoding.
func InspectImage(ctx context.Context, image string) (json.RawMessage, error) {
	if o, err := exec.CommandContext(ctx, "docker", "inspect", image).Output(); err != nil {
		return nil, fmt.Errorf("docker inspect of container image %q failed: %w", image, err)
	} else {
		return o, nil
	}
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}

func copyToTempFile(r io.Reader, mode os.FileMode) (*os.File, error) {
	tmp, err := os.CreateTemp("", "connector")
	if err != nil {
		return nil, fmt.Errorf("creating temporary file: %w", err)
	} else if _, err = io.Copy(tmp, r); err != nil {
		return nil, fmt.Errorf("copying to temporary file %s: %w", tmp.Name(), err)
	} else if err = tmp.Close(); err != nil {
		return nil, fmt.Errorf("closing temporary file %s: %w", tmp.Name(), err)
	} else if err = os.Chmod(tmp.Name(), mode); err != nil {
		return nil, fmt.Errorf("changing mode of temporary file %s: %w", tmp.Name(), err)
	}
	return tmp, nil
}

const maxMessageSize = 1 << 24 // 16 MB.
const flowConnectorInit = "flow-connector-init"
