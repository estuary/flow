package airbyte

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

// RunConnector runs the specific Docker |image| with |args|.
// Any |jsonFiles| are written as temporary files which are mounted into the
// container under "/tmp".
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
// If the container exits with a non-zero status, an error is returned containing
// a bounded prefix of the container's stderr output.
func RunConnector(
	ctx context.Context,
	image string,
	networkName string,
	args []string,
	jsonFiles map[string]interface{},
	writeLoop func(io.Writer) error,
	output io.WriteCloser,
) error {
	var imageArgs = []string{
		"docker",
		"run",
		"--interactive",
		"--rm",
	}

	if networkName != "" {
		imageArgs = append(imageArgs, fmt.Sprintf("--network=%s", networkName))
	}

	for name, m := range jsonFiles {

		// Staging location for file mounted into the container.
		var tempfile, err = ioutil.TempFile("", "connector-file")
		if err != nil {
			return fmt.Errorf("creating tempfile: %w", err)
		}

		var hostPath = tempfile.Name()
		var containerPath = filepath.Join("/tmp", name)
		defer os.RemoveAll(hostPath)

		if err := json.NewEncoder(tempfile).Encode(m); err != nil {
			return fmt.Errorf("encoding json file %q content: %w", name, err)
		} else if err = tempfile.Close(); err != nil {
			return err
		} else if err = os.Chmod(hostPath, 0644); err != nil {
			return err
		} else {
			imageArgs = append(imageArgs,
				"--mount",
				fmt.Sprintf("type=bind,source=%s,target=%s", hostPath, containerPath))
		}
	}
	args = append(append(imageArgs, image), args...)

	return runCommand(ctx, args, writeLoop, output)
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
	cmd.Stderr = &connectorStderr{}

	log.WithField("args", args).Info("invoking connector")
	if err := cmd.Start(); err != nil {
		fe.onError(fmt.Errorf("starting connector: %w", err))
	}

	// Arrange for the connector container to be signaled if |ctx| is cancelled.
	// On being signalled, docker will propagate the signal to the container
	// and wait for exit or for its shutdown timeout to elapse (10s default).
	go func(signal func(os.Signal) error) {
		<-ctx.Done()
		_ = signal(syscall.SIGTERM)
	}(cmd.Process.Signal)

	if err := cmd.Wait(); err != nil {
		fe.onError(fmt.Errorf("%w with stderr:\n\n%s",
			err, cmd.Stderr.(*connectorStderr).err.String()))
	}

	log.WithField("err", fe.unwrap()).Info("connector exited")
	_ = cmd.Stdout.(io.Closer).Close()

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

// NewConnectorProtoOutput returns an io.WriteCloser for use as
// the stdout handler of a connector. Its Write function parses
// connector output as uint32-delimited protobuf records using
// the provided message and post-decoding callback.
func NewConnectorProtoOutput(
	message proto.Message,
	onDecode func(proto.Message) error,
) io.WriteCloser {
	return &protoOutput{
		message:  message,
		onDecode: onDecode,
	}
}

type protoOutput struct {
	rem      []byte
	next     int // next body length, or zero if we're reading a header next.
	message  proto.Message
	onDecode func(proto.Message) error
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

		if o.next == 0 {
			if _, err = o.decodeLen(o.rem); err != nil {
				return 0, err
			}
		} else {
			if _, err = o.decodeMsg(o.rem); err != nil {
				return 0, err
			}
		}

		o.rem = o.rem[:0] // Truncate for re-use.
	}

	for {
		if o.next == 0 {
			if len(p) < 4 {
				o.rem = append(o.rem, p...) // We need more data.
				return n, nil
			} else if p, err = o.decodeLen(p); err != nil {
				return 0, err
			}
		} else {
			if len(p) < o.next {
				o.rem = append(o.rem, p...) // We need more data.
				return n, nil
			} else if p, err = o.decodeMsg(p); err != nil {
				return 0, err
			}
		}
	}
}

func (o *protoOutput) Close() error {
	if len(o.rem) != 0 {
		return fmt.Errorf("connector stdout closed without a final newline: %q", string(o.rem))
	}
	return nil
}

func (o *protoOutput) decodeLen(p []byte) ([]byte, error) {
	o.next = int(binary.LittleEndian.Uint32(p[:4]))
	if o.next > maxMessageSize {
		return nil, fmt.Errorf("message is too large: %d", o.next)
	}
	return p[4:], nil
}

func (o *protoOutput) decodeMsg(p []byte) ([]byte, error) {
	if err := proto.Unmarshal(p[:o.next], o.message); err != nil {
		return nil, fmt.Errorf("decoding output: %w", err)
	} else if err = o.onDecode(o.message); err != nil {
		return nil, err
	}

	p = p[o.next:]
	o.next = 0

	return p, nil
}

// NewConnectorJSONOutput returns an io.WriterCloser for use as
// the stdout handler of a connector. Its Write function parses
// connector output as newline-delimited JSON records using the
// provided initialization and post-decoding callbacks.
func NewConnectorJSONOutput(
	newRecord func() interface{},
	onDecode func(interface{}) error,
) io.WriteCloser {

	return &jsonOutput{
		newRecord: newRecord,
		onDecode:  onDecode,
	}
}

type jsonOutput struct {
	rem       []byte
	newRecord func() interface{}
	onDecode  func(interface{}) error
}

func (o *jsonOutput) Write(p []byte) (int, error) {
	var n = len(p)

	// Consume a remainder of |rem| stitched with |p|.
	if len(o.rem) != 0 {
		if ind := bytes.IndexByte(p, '\n') + 1; ind == 0 {
			// Still no newline.
			if nn := len(o.rem) + len(p); nn > maxMessageSize {
				return 0, fmt.Errorf("message is too large (%d bytes without a newline)", nn)
			} else {
				o.rem = append(o.rem, p...)
				return n, nil
			}
		} else {
			// Copy in |p| through its first newline, and parse.
			if err := o.parse(append(o.rem, p[:ind]...)); err != nil {
				return 0, err
			}
			p = p[ind:]
		}
	}
	// Consume newline frames of |p|.
	if ind := bytes.LastIndexByte(p, '\n') + 1; ind != 0 {
		if err := o.parse(p[:ind]); err != nil {
			return 0, err
		}
		p = p[ind:]
	}
	// Preserve any remainder of |p|.
	o.rem = append(o.rem[:0], p...)

	return n, nil
}

func (o *jsonOutput) parse(chunk []byte) error {
	var dec = json.NewDecoder(bytes.NewReader(chunk))
	dec.DisallowUnknownFields()

	for {
		var rec = o.newRecord()

		if err := dec.Decode(rec); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("decoding connector record: %w", err)
		} else if err = o.onDecode(rec); err != nil {
			return err
		}
	}
}

func (o *jsonOutput) Close() error {
	if len(o.rem) != 0 {
		return fmt.Errorf("connector stdout closed without a final newline: %q", string(o.rem))
	}
	return nil
}

type connectorStderr struct {
	err bytes.Buffer
}

func (s *connectorStderr) Write(p []byte) (int, error) {
	var n = len(p)
	var rem = maxStderrBytes - s.err.Len()

	if rem < n {
		p = p[:rem]
	}
	s.err.Write(p)
	return n, nil
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

const maxStderrBytes = 4096
const maxMessageSize = 1 << 23 // 8 MB.
