package airbyte

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

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
// |newRecord| returns a new, zero-valued message which is JSON-decoded into.
// |onRecord| is called with each such decoded message from the container's stdout.
//
// If |writeLoop| or |onRecord| return an error, or if the context is cancelled,
// the container is sent a SIGTERM and the error is returned.
//
// If the container exits with a non-zero status, an error is returned containing
// a bounded prefix of the container's stderr output.
func RunConnector(
	ctx context.Context,
	image string,
	args []string,
	jsonFiles map[string]interface{},
	writeLoop func(io.Writer) error,
	newRecord func() interface{},
	onRecord func(interface{}) error,
) error {

	// Staging location for files mounted into the container.
	var tempdir, err = ioutil.TempDir("", "connector-file")
	if err != nil {
		return fmt.Errorf("creating tempdir: %w", err)
	}
	defer os.RemoveAll(tempdir)

	var imageArgs = []string{
		"docker",
		"run",
		"--rm",
	}
	for name, m := range jsonFiles {
		var hostPath = filepath.Join(tempdir, name)
		var containerPath = filepath.Join("/tmp", name)

		if content, err := json.Marshal(m); err != nil {
			return fmt.Errorf("encoding json file %q content: %w", name, err)
		} else if err = os.WriteFile(hostPath, content, 0644); err != nil {
			return fmt.Errorf("staging temporary file %s: %w", name, err)
		} else {
			log.WithFields(log.Fields{
				"name":    name,
				"content": string(content),
			}).Debug("wrote connector file")

			imageArgs = append(imageArgs,
				"--mount",
				fmt.Sprintf("type=bind,source=%s,target=%s", hostPath, containerPath))
		}
	}
	args = append(append(imageArgs, image), args...)

	var cmd = exec.Command(args[0], args[1:]...)
	var fe = new(firstError)

	// On context cancellation, signal the connector to exit.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Copy |writeLoop| into connector stdin.
	wc, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("StdinPipe: %w", err)
	}
	go func() {
		defer wc.Close()
		fe.onError(writeLoop(wc))
	}()

	// Decode and forward connector stdout to |onRecord|.
	cmd.Stdout = &connectorStdout{
		onNew:    newRecord,
		onDecode: onRecord,
		onError: func(err error) {
			fe.onError(err)
			cancel() // Signal to exit.
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

	log.WithField("fe", fe.unwrap()).Info("connector exited")
	_ = cmd.Stdout.(io.Closer).Close()

	return fe.unwrap()
}

type connectorStdout struct {
	rem      []byte
	onNew    func() interface{}
	onDecode func(interface{}) error
	onError  func(error)
}

func (r *connectorStdout) Write(p []byte) (int, error) {
	if len(r.rem) == 0 {
		r.rem = append([]byte(nil), p...) // Clone.
	} else {
		r.rem = append(r.rem, p...)
	}

	var ind = bytes.LastIndexByte(r.rem, '\n') + 1
	var chunk = r.rem[:ind]
	r.rem = r.rem[ind:]

	var dec = json.NewDecoder(bytes.NewReader(chunk))
	dec.DisallowUnknownFields()

	for {
		var rec = r.onNew()

		if err := dec.Decode(rec); err == io.EOF {
			return len(p), nil
		} else if err != nil {
			r.onError(fmt.Errorf("decoding connector record: %w", err))
			return len(p), nil
		} else if err = r.onDecode(rec); err != nil {
			r.onError(err)
			return len(p), nil
		}
	}
}

func (r *connectorStdout) Close() error {
	if len(r.rem) != 0 {
		r.onError(fmt.Errorf("connector stdout closed without a final newline: %q", string(r.rem)))
	}
	return nil
}

type connectorStderr struct {
	err bytes.Buffer
}

func (r *connectorStderr) Write(p []byte) (int, error) {
	var n = len(p)
	var rem = maxStderrBytes - r.err.Len()

	if rem < n {
		p = p[:rem]
	}
	r.err.Write(p)
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
