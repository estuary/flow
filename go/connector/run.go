package connector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"

	"github.com/estuary/flow/go/flow/ops"
	"github.com/estuary/flow/go/pkgbin"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"
)

type Protocol int

const (
	Capture Protocol = iota
	Materialize
)

// proxyCommand returns a ProxyCommand of crates/connector_proxy/src/main.rs
func (c Protocol) proxyCommand() string {
	switch c {
	case Capture:
		return "proxy-flow-capture"
	case Materialize:
		return "proxy-flow-materialize"
	default:
		panic(fmt.Sprintf("go.estuary.dev/E100: unexpected protocol %v", c))
	}
}

// Run the given Docker |image| with |args|.
//
// |writeLoop| is called with a Writer that's connected to the container's stdin.
// The callback should produce input into the Writer, and then return when all
// input has been produced and the container's stdin is to be closed.
//
// |output| is an io.WriteCloser which is fed chunks of the connector's output,
// and is Closed upon the connector's exit.
//
// If |writeLoop| or |output| return an error, or if the context is cancelled,
// the container is sent a SIGTERM and the error is returned.
//
// If the container exits with a non-zero status then an error is returned
// containing a portion of the container's stderr output.
//
// Run is docker-from-docker friendly: it does require that files written to
// $TMPDIR may be mounted into and read by the container, but does not mount
// any other paths.
func Run(
	ctx context.Context,
	image string,
	protocol Protocol,
	networkName string,
	containerName string,
	args []string,
	writeLoop func(io.Writer) error,
	output io.WriteCloser,
	logger ops.Logger,
) error {
	var tmpProxy, tmpInspect *os.File

	// Copy `flowConnectorProxy` binary to $TMPDIR, from where it may be
	// mounted into the connector.
	if rPath, err := pkgbin.Locate(flowConnectorProxy); err != nil {
		return fmt.Errorf("go.estuary.dev/E101: finding %q binary: %w", flowConnectorProxy, err)
	} else if r, err := os.Open(rPath); err != nil {
		return fmt.Errorf("go.estuary.dev/E102: opening %s: %w", rPath, err)
	} else if tmpProxy, err = copyToTempFile(r, 0555); err != nil {
		return fmt.Errorf("go.estuary.dev/E103: copying %s to tmpfile: %w", rPath, err)
	}
	defer os.Remove(tmpProxy.Name())

	// Pull and inspect the image, saving its output for mounting within the container.
	if err := PullImage(ctx, image); err != nil {
		return err
	} else if out, err := InspectImage(ctx, image); err != nil {
		return err
	} else if tmpInspect, err = copyToTempFile(bytes.NewReader(out), 0444); err != nil {
		return fmt.Errorf("go.estuary.dev/E104: writing image inspect output: %w", err)
	}
	defer os.Remove(tmpInspect.Name())

	// If `networkName` is undefined, use an explicit of "bridge".
	// This is docker run's default behavior if --network is not provided.
	if networkName == "" {
		networkName = "bridge"
	}

	var imageArgs = []string{
		"docker",
		"run",
		// --init is needed in order to ensure that connector processes actually all stop when we
		// send them a SIGTERM. Without this, the (potentially numerous) child processes within a
		// container may never actually be stopped.
		"--init",
		// --interactive causes docker run to attach and proxy stdin to the container.
		"--interactive",
		// Remove the docker container upon its exit.
		"--rm",
		// Tell docker not to persist any container stdout/stderr output.
		// Containers may write _lots_ of output to std streams, and docker's
		// logging drivers may persist all or some of that to disk, which could
		// easily exhaust all available disk space. The default logging driver
		// does this. Setting the log driver here means that we don't rely on
		// any user-defined docker configuration for this, but it also means
		// that running `docker logs` to see the output of a connector will not
		// work. This is acceptable, since all of the stderr output is logged
		// into the ops collections.
		"--log-driver", "none",
		// Network to which the container should attach.
		"--network", networkName,
		// The entrypoint into a connector is always `flow-connector-proxy`,
		// which will delegate to the actual entrypoint of the connector.
		"--entrypoint", "/flow-connector-proxy",
		// Mount the connector-proxy binary, as well as the output of inspecting the docker image.
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/flow-connector-proxy", tmpProxy.Name()),
		"--mount", fmt.Sprintf("type=bind,source=%s,target=/image-inspect.json", tmpInspect.Name()),
	}

	// Name the container to avoid duplicate connectors.
	// Container names must match [a-zA-Z0-9][a-zA-Z0-9_.-]+.
	// Set SHA hash of name+command as the container name and readable
	// shard/task ID, command labels.
	if containerName != "" {
		hash := sha256.New()
		hash.Write([]byte(containerName))

		imageArgs = append(imageArgs,
			"--label", fmt.Sprintf("shard=%s", containerName))
		if len(args) > 0 {
			imageArgs = append(imageArgs,
				"--label", fmt.Sprintf("command=%s", args[0]))
			hash.Write([]byte(args[0]))
		}

		imageArgs = append(imageArgs, "--name", fmt.Sprintf("%x", hash.Sum(nil)))
	}

	imageArgs = append(imageArgs,
		image,
		// Arguments following `image` are arguments of the connector proxy and not of docker:
		"--image-inspect-json-path=/image-inspect.json",
		"--log.level", ops.LogrusToFlowLevel(logger.Level()).String(),
		protocol.proxyCommand())

	logger = ops.NewLoggerWithFields(logger, logrus.Fields{
		ops.LogSourceField: image,
		"operation":        strings.Join(args, " "),
	})

	return runCommand(ctx, append(imageArgs, args...), writeLoop, output, logger)
}

// runCommand is a lower-level API for running an executable with arguments,
// where args[0] is names the executable and args[1:] are its arguments.
// See RunConnector for details regarding treatment of |writeLoop| and |output|.
//
// It may make sense to export runCommand, but there isn't an immediate use case.
// Regardless, it exists to separate Docker concerns from the lower-level driving
// of an executable.
func runCommand(
	ctx context.Context,
	args []string,
	writeLoop func(io.Writer) error,
	output io.WriteCloser,
	logger ops.Logger,
) error {
	// Don't undertake expensive operations if we're already shutting down.
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// We use Command instead of CommandContext because we send a SIGTERM
	// rather than a SIGKILL on context cancellation. Note that `docker`
	// already has handling for propagating graceful termination with a
	// SIGKILL timeout, that we want to re-use rather than rolling our own.
	var cmd = exec.Command(args[0], args[1:]...)
	var fe = new(firstError)

	// Copy |writeLoop| into connector stdin.
	wc, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("StdinPipe: %w", err)
	}
	go func() {
		defer wc.Close()
		fe.onError(writeLoop(wc))
	}()

	var stderrForwarder = ops.NewLogForwardWriter("connector stderr", logrus.InfoLevel, logger)

	// Decode and forward connector stdout to |output|, but intercept a
	// returned error to cancel our context and report through |fe|.
	// If we didn't cancel, then the connector would run indefinitely.
	cmd.Stdout = &writeErrInterceptor{
		delegate: output,
		onError: func(err error) error {
			fe.onError(err)
			cancel() // Signal to exit.
			return err
		},
	}
	cmd.Stderr = &connectorStderr{delegate: stderrForwarder}

	logger.Log(logrus.InfoLevel, logrus.Fields{"args": args}, "invoking connector")
	if err := cmd.Start(); err != nil {
		fe.onError(fmt.Errorf("go.estuary.dev/E105: starting connector: %w", err))
	}

	// Arrange for the connector container to be signaled if |ctx| is cancelled.
	// On being signalled, docker will propagate the signal to the container
	// and wait for exit or for its shutdown timeout to elapse (10s default).
	go func(signal func(os.Signal) error) {
		<-ctx.Done()
		logger.Log(logrus.DebugLevel, nil, "sending termination signal to connector")
		if sigErr := signal(syscall.SIGTERM); sigErr != nil && sigErr != os.ErrProcessDone {
			logger.Log(logrus.WarnLevel, logrus.Fields{"error": sigErr},
				"go.estuary.dev/E106: failed to send signal to container process")
		}
	}(cmd.Process.Signal)

	err = cmd.Wait()
	var closeErr = cmd.Stdout.(io.Closer).Close()
	// Ignore error on closing stderr because it's already logged by the forwarder
	_ = stderrForwarder.Close()

	if err == nil {
		// Expect clean output after a clean exit, regardless of cancellation status.
		fe.onError(closeErr)
	} else if ctx.Err() == nil {
		// Expect a clean exit if the context wasn't cancelled.
		// Log the raw error, since we've already logged everything that was printed to stderr.
		logger.Log(logrus.ErrorLevel, logrus.Fields{"error": err}, "connector failed")
		fe.onError(fmt.Errorf("go.estuary.dev/E116: connector failed, with error: %w\nwith stderr:\n\n%s",
			err, cmd.Stderr.(*connectorStderr).buffer.String()))
	} else {
		fe.onError(ctx.Err())
	}

	logger.Log(logrus.InfoLevel, logrus.Fields{
		"error":     fe.unwrap(),
		"cancelled": ctx.Err() != nil,
	}, "connector exited")

	return fe.unwrap()
}

type writeErrInterceptor struct {
	delegate io.WriteCloser
	onError  func(error) error
}

func (w *writeErrInterceptor) Write(p []byte) (int, error) {
	n, err := w.delegate.Write(p)
	if err != nil {
		return n, w.onError(err)
	}
	return n, nil
}

func (w *writeErrInterceptor) Close() error {
	if err := w.delegate.Close(); err != nil {
		return w.onError(err)
	}
	return nil
}

// connectorStderr retains a prefix of stderr output to use for creating error messages when
// connectors exit abnormally. All output is forwarded to the delegate.
type connectorStderr struct {
	delegate io.Writer
	buffer   bytes.Buffer
}

func (s *connectorStderr) Write(p []byte) (int, error) {
	var rem = maxStderrBytes - s.buffer.Len()
	if rem > len(p) {
		rem = len(p)
	}
	s.buffer.Write(p[:rem])

	return s.delegate.Write(p)
}

// NewProtoOutput returns an io.WriteCloser for use as
// the stdout handler of a connector. Its Write function parses
// connector output as uint32-delimited protobuf records using
// the provided new message and post-decoding callbacks.
func NewProtoOutput(
	newRecord func() proto.Message,
	onDecode func(proto.Message) error,
) io.WriteCloser {
	return &protoOutput{
		newRecord: newRecord,
		onDecode:  onDecode,
	}
}

type protoOutput struct {
	rem       []byte
	next      int // next body length, or zero if we're reading a header next.
	newRecord func() proto.Message
	onDecode  func(proto.Message) error
}

func (o *protoOutput) Write(p []byte) (n int, err error) {
	n = len(p)

	// Do we have a remainder which must be stitched with |p|?
	if len(o.rem) != 0 {
		// How much more data do we need for the next header or body ?
		var need int
		if o.next != 0 {
			need = o.next // |o.rem| holds a partial body of length |o.next|.
		} else {
			need = 4 // |o.rem| holds a partial 4-byte header.
		}

		var delta = need - len(o.rem)
		if delta > len(p) {
			o.rem = append(o.rem, p...)
			return n, nil // We still need more data.
		}

		// Stitch |delta| bytes from head of |p| onto |o.rem|.
		o.rem = append(o.rem, p[:delta]...)
		p = p[delta:]

		if r, err := o.decode(o.rem); len(r) != 0 {
			panic("didn't consume stitched remainder")
		} else if err != nil {
			return 0, err
		}

		o.rem = o.rem[:0] // Truncate for re-use.
	}

	for len(p) != 0 {
		if p, err = o.decode(p); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (o *protoOutput) Close() error {
	if len(o.rem) != 0 {
		return fmt.Errorf("go.estuary.dev/E107: connector stdout closed without a final newline: %q", string(o.rem))
	}
	return nil
}

func (o *protoOutput) decode(p []byte) ([]byte, error) {
	if o.next == 0 {
		if len(p) < 4 {
			o.rem = append(o.rem, p...) // We need more data.
			return nil, nil
		}

		// Consume 4 byte header.
		o.next = int(binary.LittleEndian.Uint32(p[:4]))
		p = p[4:]

		if o.next > maxMessageSize {
			return nil, fmt.Errorf("go.estuary.dev/E108: message is too large: %d", o.next)
		}

		// Fall through to attempt decode of the message.
		// Note that explicit, zero-length messages are a possibility.
		// Falling through correctly handles this case.
	}

	if len(p) < o.next {
		o.rem = append(o.rem, p...) // We need more data.
		return nil, nil
	}

	// Consume |o.next| length message.
	var m = o.newRecord()
	if err := proto.Unmarshal(p[:o.next], m); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E109: decoding output: %w", err)
	} else if err = o.onDecode(m); err != nil {
		return nil, err
	}

	p = p[o.next:]
	o.next = 0

	return p, nil
}

type firstError struct {
	err error
	mu  sync.Mutex
}

func (fe *firstError) onError(err error) {
	defer fe.mu.Unlock()
	fe.mu.Lock()

	if fe.err == nil {
		fe.err = err
	}
}

func (fe *firstError) unwrap() error {
	defer fe.mu.Unlock()
	fe.mu.Lock()

	return fe.err
}

// PullImage to local cache unless the tag is `:local`, which is expected to be local.
func PullImage(ctx context.Context, image string) error {
	// Pull the image if it's not expected to be local.
	if strings.HasSuffix(image, ":local") {
		// Don't pull images having this tag.
	} else if _, err := exec.CommandContext(ctx, "docker", "pull", "--quiet", image).Output(); err != nil {
		return fmt.Errorf("go.estuary.dev/E110: pull of container image %q failed: %w", image, err)
	}
	return nil
}

// InspectImage and return its Docker-compatible metadata JSON encoding.
func InspectImage(ctx context.Context, image string) (json.RawMessage, error) {
	if o, err := exec.CommandContext(ctx, "docker", "inspect", image).Output(); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E111: inspection of container image %q failed: %w", image, err)
	} else {
		return o, nil
	}
}

func copyToTempFile(r io.Reader, mode os.FileMode) (*os.File, error) {
	tmp, err := os.CreateTemp("", "connector")
	if err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E112: creating tempfile: %w", err)
	} else if _, err = io.Copy(tmp, r); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E113: copying to tempfile %s: %w", tmp.Name(), err)
	} else if err = tmp.Close(); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E114: closing tempfile %s: %w", tmp.Name(), err)
	} else if err = os.Chmod(tmp.Name(), mode); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E115: chmod of tempfile %s: %w", tmp.Name(), err)
	}
	return tmp, nil
}

const maxStderrBytes = 4096
const maxMessageSize = 1 << 23 // 8 MB.
const flowConnectorProxy = "flow-connector-proxy"
