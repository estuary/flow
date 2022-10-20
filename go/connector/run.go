package connector

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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

const ConnectorTCPPortLabel = "FLOW_TCP_PORT"

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
	args []string,
	writeLoop func(io.Writer) error,
	output io.WriteCloser,
	logger ops.Logger,
) error {
	var imageInspectRaw json.RawMessage
	var inspects = []ImageInspect{ImageInspect{}}
	// Pull and inspect the image, saving its output for mounting within the container.
	if err := PullImage(ctx, image); err != nil {
		return err
	} else if imageInspectRaw, err = DockerInspect(ctx, image); err != nil {
		return err
	} else if err := json.Unmarshal(imageInspectRaw, &inspects); err != nil {
		return fmt.Errorf("go.estuary.dev/E132: parsing container image %q inspect results: %w", image, err)
	}

	var tmpProxy, tmpInspect *os.File

	// Find out the port on which the connector will be listening. If there is no
	// port specified, then fallback to using connector_proxy and stdio
	var port = inspects[0].Config.Labels[ConnectorTCPPortLabel]
	if port == "" {
		logger.Log(logrus.WarnLevel, logrus.Fields{}, "go.estuary.dev/W002: container did not specify port label, using stdio. Using stdio is deprecated and will be removed in the future.")

		// Copy `flowConnectorProxy` binary to $TMPDIR, from where it may be
		// mounted into the connector.
		if rPath, err := pkgbin.Locate(flowConnectorProxy); err != nil {
			return fmt.Errorf("go.estuary.dev/E101: finding %q binary: %w", flowConnectorProxy, err)
		} else if r, err := os.Open(rPath); err != nil {
			return fmt.Errorf("go.estuary.dev/E102: opening %s: %w", rPath, err)
		} else if tmpProxy, err = copyToTempFile(r, 0555); err != nil {
			return fmt.Errorf("go.estuary.dev/E103: copying %s to tmpfile: %w", rPath, err)
		} else if tmpInspect, err = copyToTempFile(bytes.NewReader(imageInspectRaw), 0444); err != nil {
			return fmt.Errorf("go.estuary.dev/E104: writing image inspect output: %w", err)
		}
		defer os.Remove(tmpProxy.Name())
		defer os.Remove(tmpInspect.Name())
	}

	// If `networkName` is undefined, use an explicit of "bridge".
	// This is docker run's default behavior if --network is not provided.
	if networkName == "" {
		networkName = "bridge"
	}

	// Find a free port on local system
	var localAddress string
	if port != "" {
		localPort, err := GetFreePort()
		if err != nil {
			return fmt.Errorf("go.estuary.dev/E133: could not get a free port on host: %w", err)
		}

		localAddress = fmt.Sprintf("127.0.0.1:%s", localPort)
	}

	var imageArgs = []string{
		"docker",
		"run",
		// --init is needed in order to ensure that connector processes actually all stop when we
		// send them a SIGTERM. Without this, the (potentially numerous) child processes within a
		// container may never actually be stopped.
		"--init",
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
	}

	if port == "" {
		imageArgs = append(imageArgs,
			// The entrypoint into a connector is always `flow-connector-proxy`,
			// which will delegate to the actual entrypoint of the connector.
			"--entrypoint", "/flow-connector-proxy",
			// Mount the connector-proxy binary, as well as the output of inspecting the docker image.
			"--mount", fmt.Sprintf("type=bind,source=%s,target=/flow-connector-proxy", tmpProxy.Name()),
			"--mount", fmt.Sprintf("type=bind,source=%s,target=/image-inspect.json", tmpInspect.Name()),
			"--interactive",
		)
	} else {
		// Publish the tcp port of the container on a random port on host
		imageArgs = append(imageArgs,
			"--publish", fmt.Sprintf("%s:%s", localAddress, port),
		)
	}

	imageArgs = append(imageArgs,
		image,
		"--log.level", ops.LogrusToFlowLevel(logger.Level()).String(),
	)

	if port == "" {
		imageArgs = append(imageArgs,
			// Arguments following `image` are arguments of the connector proxy and not of docker:
			"--image-inspect-json-path=/image-inspect.json",
			protocol.proxyCommand(),
		)
	}

	logger = ops.NewLoggerWithFields(logger, logrus.Fields{
		ops.LogSourceField: image,
		"operation":        strings.Join(args, " "),
	})

	return runCommand(ctx, append(imageArgs, args...), localAddress, port, writeLoop, output, logger)
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
	localAddress string,
	port string,
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

	// Decode and forward connector output from socket to |output|, but intercept a
	// returned error to cancel our context and report through |fe|.
	// If we didn't cancel, then the connector would run indefinitely.
	var outputInterceptor = &writeErrInterceptor{
		delegate: output,
		onError: func(err error) error {
			fe.onError(err)
			cancel() // Signal to exit.
			return err
		},
	}

	var group = sync.WaitGroup{}
	var conn net.Conn
	// If port is specified, use TCP socket
	if port != "" {
		// We're going to need to await 3 separate async tasks:
		// The attempt to dial, and the two copy operations in
		// and out of the established connection.
		group.Add(3)
		// Routine dialing a connection to connector
		go func() {
			// Try to connect to the connector with a retry mechanism
			// Don't retry on context cancellation
			// Retry after a short wait
			conn, connErr := connectTCP(ctx, conn, localAddress)
			group.Add(-1)
			if connErr != nil {
				fe.onError(connErr)
				// We're not going to run the copy goroutines, so decrement
				// the waitgroup now.
				group.Done()
				group.Done()
				return
			}

			// Copy |writeLoop| into socket
			go func() {
				fe.onError(writeLoop(conn))
				os.Stderr.WriteString("writeLoop end\n")
				group.Done()
			}()

			// Read from socket connection and delegate to output through the error interceptor
			go func() {
				var _, err = io.Copy(outputInterceptor, conn)
				os.Stderr.WriteString("outputInterceptor write end\n")
				fe.onError(err)
				group.Done()
			}()
		}()
	} else {
		// Otherwise, use the old stdio channels

		// Copy |writeLoop| into connector stdin.
		wc, err := cmd.StdinPipe()
		if err != nil {
			return fmt.Errorf("StdinPipe: %w", err)
		}
		go func() {
			defer wc.Close()
			fe.onError(writeLoop(wc))
		}()

		cmd.Stdout = outputInterceptor
	}

	var stderrForwarder = ops.NewLogForwardWriter("connector stderr", logrus.InfoLevel, logger)

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

	if waitErr := cmd.Wait(); waitErr != nil {
		if ctx.Err() == nil {
			// Expect a clean exit if the context wasn't cancelled.
			// Log the raw error, since we've already logged everything that was printed to stderr.
			logger.Log(logrus.ErrorLevel, logrus.Fields{"error": waitErr}, "connector failed")
			fe.onError(fmt.Errorf("go.estuary.dev/E116: connector failed, with error: %w\nwith stderr:\n\n%s",
				waitErr, cmd.Stderr.(*connectorStderr).buffer.String()))
		} else {
			fe.onError(ctx.Err())
		}
	}
	os.Stderr.WriteString("cmd.Wait done\n")
	// Wait for any TCP copy operations to finish. This must be done after the process exits.
	group.Wait()
	os.Stderr.WriteString("group.Wait done\n")
	_ = stderrForwarder.Close()
	os.Stderr.WriteString("stderrForwarder closed\n")
	_ = output.Close()
	os.Stderr.WriteString("output closed\n")

	if port != "" && conn != nil {
		var closeErr = conn.Close()
		if closeErr != nil {
			fe.onError(closeErr)
		}
	}

	logger.Log(logrus.InfoLevel, logrus.Fields{
		"error":     fe.unwrap(),
		"cancelled": ctx.Err() != nil,
	}, "connector exited")

	return fe.unwrap()
}

func connectTCP(ctx context.Context, conn net.Conn, localAddress string) (net.Conn, error) {
	var connectDeadline = time.Now().Add(time.Second * 10)
	var err error
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var dialer = net.Dialer{
			Timeout: time.Second * 10,
		}
		conn, err = dialer.DialContext(ctx, "tcp", localAddress)
		if err == nil {
			return conn, nil
		}

		if time.Now().After(connectDeadline) {
			return nil, fmt.Errorf("dialing connection to %s: %w", localAddress, err)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
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

type ImageConfig struct {
	Labels map[string]string
}

type ImageInspect struct {
	Config *ImageConfig
}

// DockerInspect and return its Docker-compatible metadata JSON encoding.
func DockerInspect(ctx context.Context, entity string) (json.RawMessage, error) {
	if o, err := exec.CommandContext(ctx, "docker", "inspect", entity).Output(); err != nil {
		return nil, fmt.Errorf("go.estuary.dev/E111: inspection of docker entity %q failed: %w", entity, err)
	} else {
		return o, nil
	}
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func GetFreePort() (port string, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
		}
	}
	return
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
