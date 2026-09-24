package parser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

const ProgramName = "flow-parser"

// ParseStream invokes the parser using the given config file and Reader of data to parse.
// The data is piped to the parser via stdin. This is the right choice for streaming formats
// (CSV/JSON/NDJSON), where parsing can overlap with an in-flight download.
//
// The callback is called as JSON documents are emitted by the parser, and receives
// batches of documents in JSON format. Each record includes a single trailing newline.
// The data provided to the callback is from a shared buffer, and must not be retained after the
// callback returns. You need to copy the data if you need it longer than that.
//
// If the callback returns an error, or if the context is cancelled,
// the parser is sent a SIGTERM and the error is returned.
//
// Or if the parser exits with a non-zero status, an error is returned containing
// a bounded prefix of the container's stderr output.
func ParseStream(
	ctx context.Context,
	configPath string,
	input io.Reader,
	callback func(lines []json.RawMessage) error,
) error {
	return runParser(ctx, configPath, input, "", callback)
}

// ParseFile invokes the parser against an already-local, seekable file at inputPath, passed
// via the parser's `--file` flag rather than piped through stdin. This lets seek-oriented
// formats (parquet, Excel) read footers and row groups by random access instead of buffering
// the whole input to a temporary file. Prefer it whenever the input is already on local disk.
//
// The callback contract matches ParseStream.
func ParseFile(
	ctx context.Context,
	configPath string,
	inputPath string,
	callback func(lines []json.RawMessage) error,
) error {
	return runParser(ctx, configPath, nil, inputPath, callback)
}

// runParser executes flow-parser and dispatches its stdout to callback. Exactly one of input
// (piped to stdin) or inputPath (passed via --file) supplies the data; inputPath takes
// precedence when non-empty.
func runParser(
	ctx context.Context,
	configPath string,
	input io.Reader,
	inputPath string,
	callback func(lines []json.RawMessage) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var cmd = exec.CommandContext(ctx, ProgramName, "parse", "--config-file", configPath)
	var stdoutErr error

	if inputPath != "" {
		cmd.Args = append(cmd.Args, "--file", inputPath)
	} else {
		cmd.Stdin = input
	}

	if log.IsLevelEnabled(log.DebugLevel) {
		cmd.Args = append(cmd.Args, "--log", "debug")
	}

	cmd.Stdout = &parserStdout{
		onLines: callback,
		onError: func(err error) {
			if stdoutErr == nil {
				stdoutErr = err
			}
			cancel()
		},
	}
	cmd.Stderr = os.Stderr

	var runErr = cmd.Run()

	if stdoutErr != nil {
		return fmt.Errorf("invalid parser output: %w", stdoutErr)
	} else if runErr != nil {
		return fmt.Errorf("parser failed: %w", runErr)
	} else if len(cmd.Stdout.(*parserStdout).rem) != 0 {
		return fmt.Errorf("connector exited without a final newline")
	}

	return nil
}

// GetSpec invokes the parser to get the configuration json schema. The returned schema can then be
// included directly in a connector configuration schema if desired.
func GetSpec() (json.RawMessage, error) {
	var spec, err = exec.Command(ProgramName, "spec").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute 'parser spec': %w", err)
	}
	return json.RawMessage(spec), nil
}

// parserStdout collects lines of parser output and invokes its callback.
type parserStdout struct {
	rem     []byte
	scratch []json.RawMessage

	onLines func([]json.RawMessage) error
	onError func(error)
}

func (r *parserStdout) Write(p []byte) (int, error) {
	var n = len(p)

	// Accumulate linebreaks of |p| into |lines|.
	var lines = r.scratch[:0]
	for {
		var pivot = bytes.IndexByte(p, '\n')
		if pivot == -1 {
			break
		}
		var line = p[:pivot]

		// If there was an unconsumed remainder, prefix it into |next|.
		if len(r.rem) != 0 {
			line = append(r.rem, line...)
			r.rem = r.rem[:0]

			// Note that |lines| continues to reference |r.rem|.
		}

		lines = append(lines, line)
		p = p[pivot+1:]
	}

	err := r.onLines(lines)
	if err != nil {
		r.onError(err)
	}

	// Copy unconsumed remainder of |p| for next Write invocation.
	// Safe because onLines() doesn't retain |lines| after it returns.
	r.rem = append(r.rem, p...)
	r.scratch = lines[:0]

	// Returns the err value from onLines. This may not be nil, but we want to
	// adjust the rem/scratch before bailing.
	return n, err
}
